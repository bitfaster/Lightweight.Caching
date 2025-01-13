using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace BitFaster.Caching.Lru
{
    /// <summary>
    /// A pseudo LRU based on the TU-Q eviction policy. The LRU list is composed of 3 segments: hot, warm and cold. 
    /// Cost of maintaining segments is amortized across requests. Items are only cycled when capacity is exceeded. 
    /// Pure read does not cycle items if all segments are within capacity constraints. There are no global locks. 
    /// On cache miss, a new item is added. Tail items in each segment are dequeued, examined, and are either enqueued 
    /// or discarded.
    /// The TU-Q scheme of hot, warm and cold is similar to that used in MemCached (https://memcached.org/blog/modern-lru/)
    /// and OpenBSD (https://flak.tedunangst.com/post/2Q-buffer-cache-algorithm), but does not use a background thread
    /// to maintain the internal queues.
    /// </summary>
    /// <remarks>
    /// Each segment has a capacity. When segment capacity is exceeded, items are moved as follows:
    /// <list type="number">
    ///   <item><description>New items are added to hot, WasAccessed = false.</description></item>
    ///   <item><description>When items are accessed, update WasAccessed = true.</description></item>
    ///   <item><description>When items are moved WasAccessed is set to false.</description></item>
    ///   <item><description>When hot is full, hot tail is moved to either Warm or Cold depending on WasAccessed.</description></item>
    ///   <item><description>When warm is full, warm tail is moved to warm head or cold depending on WasAccessed.</description></item>
    ///   <item><description>When cold is full, cold tail is moved to warm head or removed from dictionary on depending on WasAccessed.</description></item>
    ///</list>
    /// </remarks>
    public class ConcurrentLruCore<K, V, I, P, T> : ICacheExt<K, V>, IAsyncCacheExt<K, V>, IEnumerable<KeyValuePair<K, V>>
        where K : notnull
        where I : LruItem<K, V>
        where P : struct, IItemPolicy<K, V, I>
        where T : struct, ITelemetryPolicy<K, V>
    {
        private readonly ConcurrentDictionary<K, I> dictionary;

        private readonly ConcurrentQueue<I> hotQueue;
        private readonly ConcurrentQueue<I> warmQueue;
        private readonly ConcurrentQueue<I> coldQueue;

        // maintain count outside ConcurrentQueue, since ConcurrentQueue.Count holds a global lock
        private PaddedQueueCount counter;

        private readonly ICapacityPartition capacity;

        private readonly P itemPolicy;
        private bool isWarm = false;

        /// <summary>
        /// The telemetry policy.
        /// </summary>
        /// <remarks>
        /// Since T is a struct, making it readonly will force the runtime to make defensive copies
        /// if mutate methods are called. Therefore, field must be mutable to maintain count.
        /// </remarks>
        protected T telemetryPolicy;

        /// <summary>
        /// Initializes a new instance of the ConcurrentLruCore class with the specified concurrencyLevel, capacity, equality comparer, item policy and telemetry policy.
        /// </summary>
        /// <param name="concurrencyLevel">The concurrency level.</param>
        /// <param name="capacity">The capacity.</param>
        /// <param name="comparer">The equality comparer.</param>
        /// <param name="itemPolicy">The item policy.</param>
        /// <param name="telemetryPolicy">The telemetry policy.</param>
        /// <exception cref="ArgumentNullException"></exception>
        public ConcurrentLruCore(
            int concurrencyLevel,
            ICapacityPartition capacity,
            IEqualityComparer<K> comparer,
            P itemPolicy,
            T telemetryPolicy)
        {
            if (capacity == null)
                Throw.ArgNull(ExceptionArgument.capacity);

            if (comparer == null)
                Throw.ArgNull(ExceptionArgument.comparer);

            capacity.Validate();
            this.capacity = capacity;

            this.hotQueue = new ConcurrentQueue<I>();
            this.warmQueue = new ConcurrentQueue<I>();
            this.coldQueue = new ConcurrentQueue<I>();

            int dictionaryCapacity = ConcurrentDictionarySize.Estimate(this.Capacity);

            this.dictionary = new ConcurrentDictionary<K, I>(concurrencyLevel, dictionaryCapacity, comparer);
            this.itemPolicy = itemPolicy;
            this.telemetryPolicy = telemetryPolicy;
            this.telemetryPolicy.SetEventSource(this);
        }

        // No lock count: https://arbel.net/2013/02/03/best-practices-for-using-concurrentdictionary/
        ///<inheritdoc/>
        public int Count => this.dictionary.Where(i => !itemPolicy.ShouldDiscard(i.Value)).Count();

        ///<inheritdoc/>
        public int Capacity => this.capacity.Hot + this.capacity.Warm + this.capacity.Cold;

        ///<inheritdoc/>
        public Optional<ICacheMetrics> Metrics => CreateMetrics(this);

        ///<inheritdoc/>
        public Optional<ICacheEvents<K, V>> Events => CreateEvents(this);

        ///<inheritdoc/>
        public CachePolicy Policy => CreatePolicy(this);

        /// <summary>
        /// Gets the number of hot items.
        /// </summary>
        public int HotCount => Volatile.Read(ref this.counter.hot);

        /// <summary>
        /// Gets the number of warm items.
        /// </summary>
        public int WarmCount => Volatile.Read(ref this.counter.warm);

        /// <summary>
        /// Gets the number of cold items.
        /// </summary>
        public int ColdCount => Volatile.Read(ref this.counter.cold);

        /// <summary>
        /// Gets a collection containing the keys in the cache.
        /// </summary>
        public ICollection<K> Keys => this.dictionary.Keys;

        /// <summary>Returns an enumerator that iterates through the cache.</summary>
        /// <returns>An enumerator for the cache.</returns>
        /// <remarks>
        /// The enumerator returned from the cache is safe to use concurrently with
        /// reads and writes, however it does not represent a moment-in-time snapshot.  
        /// The contents exposed through the enumerator may contain modifications
        /// made after <see cref="GetEnumerator"/> was called.
        /// </remarks>
        public IEnumerator<KeyValuePair<K, V>> GetEnumerator()
        {
            foreach (var kvp in this.dictionary)
            {
                if (!itemPolicy.ShouldDiscard(kvp.Value))
                { 
                    yield return new KeyValuePair<K, V>(kvp.Key, kvp.Value.Value); 
                }
            }
        }

        ///<inheritdoc/>
        public bool TryGet(K key, [MaybeNullWhen(false)] out V value)
        {
            if (dictionary.TryGetValue(key, out var item))
            {
                return GetOrDiscard(item, out value);
            }

            value = default;
            this.telemetryPolicy.IncrementMiss();
            return false;
        }

        // AggressiveInlining forces the JIT to inline policy.ShouldDiscard(). For LRU policy 
        // the first branch is completely eliminated due to JIT time constant propogation.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool GetOrDiscard(I item, [MaybeNullWhen(false)] out V value)
        {
            if (this.itemPolicy.ShouldDiscard(item))
            {
                this.Move(item, ItemDestination.Remove, ItemRemovedReason.Evicted);
                this.telemetryPolicy.IncrementMiss();
                value = default;
                return false;
            }

            value = item.Value;

            this.itemPolicy.Touch(item);
            this.telemetryPolicy.IncrementHit();
            return true;
        }

        private bool TryAdd(K key, V value)
        {
            var newItem = this.itemPolicy.CreateItem(key, value);

            if (this.dictionary.TryAdd(key, newItem))
            {
                this.hotQueue.Enqueue(newItem);
                Cycle(Interlocked.Increment(ref counter.hot));
                return true;
            }

            Disposer<V>.Dispose(newItem.Value);
            return false;
        }

        ///<inheritdoc/>
        public V GetOrAdd(K key, Func<K, V> valueFactory)
        {
            while (true)
            {
                if (this.TryGet(key, out var value))
                {
                    return value;
                }

                // The value factory may be called concurrently for the same key, but the first write to the dictionary wins.
                value = valueFactory(key);

                if (TryAdd(key, value))
                {
                    return value;
                }
            }
        }

        /// <summary>
        /// Adds a key/value pair to the cache if the key does not already exist. Returns the new value, or the 
        /// existing value if the key already exists.
        /// </summary>
        /// <typeparam name="TArg">The type of an argument to pass into valueFactory.</typeparam>
        /// <param name="key">The key of the element to add.</param>
        /// <param name="valueFactory">The factory function used to generate a value for the key.</param>
        /// <param name="factoryArgument">An argument value to pass into valueFactory.</param>
        /// <returns>The value for the key. This will be either the existing value for the key if the key is already 
        /// in the cache, or the new value if the key was not in the cache.</returns>
        public V GetOrAdd<TArg>(K key, Func<K, TArg, V> valueFactory, TArg factoryArgument)
        {
            while (true)
            {
                if (this.TryGet(key, out var value))
                {
                    return value;
                }

                // The value factory may be called concurrently for the same key, but the first write to the dictionary wins.
                value = valueFactory(key, factoryArgument);

                if (TryAdd(key, value))
                {
                    return value;
                }
            }
        }

        ///<inheritdoc/>
        public async ValueTask<V> GetOrAddAsync(K key, Func<K, Task<V>> valueFactory)
        {
            while (true)
            {
                if (this.TryGet(key, out var value))
                {
                    return value;
                }

                // The value factory may be called concurrently for the same key, but the first write to the dictionary wins.
                // This is identical logic in ConcurrentDictionary.GetOrAdd method.
                value = await valueFactory(key).ConfigureAwait(false);

                if (TryAdd(key, value))
                {
                    return value;
                }
            }
        }

        /// <summary>
        /// Adds a key/value pair to the cache if the key does not already exist. Returns the new value, or the 
        /// existing value if the key already exists.
        /// </summary>
        /// <typeparam name="TArg">The type of an argument to pass into valueFactory.</typeparam>
        /// <param name="key">The key of the element to add.</param>
        /// <param name="valueFactory">The factory function used to asynchronously generate a value for the key.</param>
        /// <param name="factoryArgument">An argument value to pass into valueFactory.</param>
        /// <returns>A task that represents the asynchronous GetOrAdd operation.</returns>
        public async ValueTask<V> GetOrAddAsync<TArg>(K key, Func<K, TArg, Task<V>> valueFactory, TArg factoryArgument)
        {
            while (true)
            {
                if (this.TryGet(key, out var value))
                {
                    return value;
                }

                // The value factory may be called concurrently for the same key, but the first write to the dictionary wins.
                value = await valueFactory(key, factoryArgument).ConfigureAwait(false);

                if (TryAdd(key, value))
                {
                    return value;
                }
            }
        }

        /// <summary>
        /// Attempts to remove the specified key value pair.
        /// </summary>
        /// <param name="item">The item to remove.</param>
        /// <returns>true if the item was removed successfully; otherwise, false.</returns>
        public bool TryRemove(KeyValuePair<K, V> item)
        {
            if (this.dictionary.TryGetValue(item.Key, out var existing))
            {
                lock (existing)
                {
                    if (EqualityComparer<V>.Default.Equals(existing.Value, item.Value))
                    {
                        var kvp = new KeyValuePair<K, I>(item.Key, existing);
#if NET6_0_OR_GREATER
                    if (this.dictionary.TryRemove(kvp))
#else
                        // https://devblogs.microsoft.com/pfxteam/little-known-gems-atomic-conditional-removals-from-concurrentdictionary/
                        if (((ICollection<KeyValuePair<K, I>>)this.dictionary).Remove(kvp))
#endif
                        {
                            OnRemove(item.Key, kvp.Value, ItemRemovedReason.Removed);
                            return true;
                        }
                    }
                }

                // it existed, but we couldn't remove - this means value was replaced afer the TryGetValue (a race)
            }

            return false;
        }

        /// <summary>
        /// Attempts to remove and return the value that has the specified key.
        /// </summary>
        /// <param name="key">The key of the element to remove.</param>
        /// <param name="value">When this method returns, contains the object removed, or the default value of the value type if key does not exist.</param>
        /// <returns>true if the object was removed successfully; otherwise, false.</returns>
        public bool TryRemove(K key, [MaybeNullWhen(false)] out V value)
        {
            if (this.dictionary.TryRemove(key, out var item))
            {
                OnRemove(key, item, ItemRemovedReason.Removed);
                value = item.Value;
                return true;
            }

            value = default;
            return false;
        }

        ///<inheritdoc/>
        public bool TryRemove(K key)
        {
            return TryRemove(key, out _);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void OnRemove(K key, I item, ItemRemovedReason reason)
        {
            // Mark as not accessed, it will later be cycled out of the queues because it can never be fetched 
            // from the dictionary. Note: Hot/Warm/Cold count will reflect the removed item until it is cycled 
            // from the queue.
            item.WasAccessed = false;
            item.WasRemoved = true;

            this.telemetryPolicy.OnItemRemoved(key, item.Value, reason);

            // serialize dispose (common case dispose not thread safe)
            lock (item)
            {
                Disposer<V>.Dispose(item.Value);
            }
        }

        ///<inheritdoc/>
        ///<remarks>Note: Calling this method does not affect LRU order.</remarks>
        public bool TryUpdate(K key, V value)
        {
            if (this.dictionary.TryGetValue(key, out var existing))
            {
                lock (existing)
                {
                    if (!existing.WasRemoved)
                    {
                        V oldValue = existing.Value;

                        existing.Value = value;

                        this.itemPolicy.Update(existing);
// backcompat: remove conditional compile
#if NETCOREAPP3_0_OR_GREATER
                        this.telemetryPolicy.OnItemUpdated(existing.Key, oldValue, existing.Value);
#endif
                        Disposer<V>.Dispose(oldValue);

                        return true;
                    }
                }
            }

            return false;
        }

        ///<inheritdoc/>
        ///<remarks>Note: Updates to existing items do not affect LRU order. Added items are at the top of the LRU.</remarks>
        public void AddOrUpdate(K key, V value)
        {
            while (true)
            {
                // first, try to update
                if (this.TryUpdate(key, value))
                {
                    return;
                }

                // then try add
                var newItem = this.itemPolicy.CreateItem(key, value);

                if (this.dictionary.TryAdd(key, newItem))
                {
                    this.hotQueue.Enqueue(newItem);
                    Cycle(Interlocked.Increment(ref counter.hot));
                    return;
                }

                // if both update and add failed there was a race, try again
            }
        }

        ///<inheritdoc/>
        public void Clear()
        {
            // don't overlap Clear/Trim/TrimExpired
            lock (this.dictionary)
            {
                // evaluate queue count, remove everything including items removed from the dictionary but
                // not the queues. This also avoids the expensive o(n) no lock count, or locking the dictionary.
                int queueCount = this.HotCount + this.WarmCount + this.ColdCount;
                this.TrimLiveItems(itemsRemoved: 0, queueCount, ItemRemovedReason.Cleared);
            }
        }

        /// <summary>
        /// Trim the specified number of items from the cache. Removes all discardable items per IItemPolicy.ShouldDiscard(), then 
        /// itemCount-discarded items in LRU order, if any.
        /// </summary>
        /// <param name="itemCount">The number of items to remove.</param>
        /// <returns>The number of items removed from the cache.</returns>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="itemCount"/> is less than 0./</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="itemCount"/> is greater than capacity./</exception>
        /// <remarks>
        /// Note: Trim affects LRU order. Calling Trim resets the internal accessed status of items.
        /// </remarks>
        public void Trim(int itemCount)
        {
            int capacity = this.Capacity;

            if (itemCount < 1 || itemCount > capacity)
                Throw.ArgOutOfRange(nameof(itemCount), "itemCount must be greater than or equal to one, and less than the capacity of the cache.");

            // clamp itemCount to number of items actually in the cache
            itemCount = Math.Min(itemCount, this.HotCount + this.WarmCount + this.ColdCount);

            // don't overlap Clear/Trim/TrimExpired
            lock (this.dictionary)
            {
                // first scan each queue for discardable items and remove them immediately. Note this can remove > itemCount items.
                int itemsRemoved = this.itemPolicy.CanDiscard() ? TrimAllDiscardedItems() : 0;

                TrimLiveItems(itemsRemoved, itemCount, ItemRemovedReason.Trimmed);
            }
        }

        private void TrimExpired()
        {
            if (this.itemPolicy.CanDiscard())
            {
                this.TrimAllDiscardedItems();
            }
        }

        /// <summary>
        /// Trim discarded items from all queues.
        /// </summary>
        /// <returns>The number of items removed.</returns>
        // backcompat: make internal
        protected int TrimAllDiscardedItems()
        {
            // don't overlap Clear/Trim/TrimExpired
            lock (this.dictionary)
            {
                int RemoveDiscardableItems(ConcurrentQueue<I> q, ref int queueCounter)
                {
                    int itemsRemoved = 0;
                    int localCount = queueCounter;

                    for (int i = 0; i < localCount; i++)
                    {
                        if (q.TryDequeue(out var item))
                        {
                            if (this.itemPolicy.ShouldDiscard(item))
                            {
                                Interlocked.Decrement(ref queueCounter);
                                this.Move(item, ItemDestination.Remove, ItemRemovedReason.Trimmed);
                                itemsRemoved++;
                            }
                            else
                            {
                                q.Enqueue(item);
                            }
                        }
                    }

                    return itemsRemoved;
                }

                int coldRem = RemoveDiscardableItems(coldQueue, ref this.counter.cold);
                int warmRem = RemoveDiscardableItems(warmQueue, ref this.counter.warm);
                int hotRem = RemoveDiscardableItems(hotQueue, ref this.counter.hot);

                if (warmRem > 0)
                {
                    Volatile.Write(ref this.isWarm, false);
                }

                return coldRem + warmRem + hotRem;
            }
        }

        private void TrimLiveItems(int itemsRemoved, int itemCount, ItemRemovedReason reason)
        {
            // When items are touched, they are moved to warm by cycling. Therefore, to guarantee 
            // that we can remove itemCount items, we must cycle (2 * capacity.Warm) + capacity.Hot times.
            // If clear is called during trimming, it would be possible to get stuck in an infinite
            // loop here. The warm + hot limit also guards against this case.
            int trimWarmAttempts = 0;
            int maxWarmHotAttempts = (this.capacity.Warm * 2) + this.capacity.Hot;

            int warmCount = Volatile.Read(ref this.counter.warm);
            int coldCount = Volatile.Read(ref this.counter.cold);

            while (itemsRemoved < itemCount && trimWarmAttempts < maxWarmHotAttempts)
            {
                if (coldCount > 0)
                {
                    if (TryRemoveCold(reason) == (ItemDestination.Remove, 0))
                    {
                        itemsRemoved++;
                        coldCount--;
                        trimWarmAttempts = 0;
                    }

                    TrimWarmOrHot(reason, ref warmCount, ref coldCount);
                }
                else
                {
                    TrimWarmOrHot(reason, ref warmCount, ref coldCount);
                    trimWarmAttempts++;
                }
            }

            if (warmCount < this.capacity.Warm)
            {
                Volatile.Write(ref this.isWarm, false);
            }
        }

        private void TrimWarmOrHot(ItemRemovedReason reason, ref int warmCount, ref int coldCount)
        {
            if (warmCount > 0)
            {
                var (dest, count) = CycleWarmUnchecked(reason);
                warmCount--;
                UpdateCount(ref warmCount, ref coldCount, dest, count);
            }
            else if (Volatile.Read(ref this.counter.hot) > 0)
            {
                var (dest, count) = CycleHotUnchecked(reason);
                UpdateCount(ref warmCount, ref coldCount, dest, count);
            }

            void UpdateCount(ref int warmCount, ref int coldCount, ItemDestination dest, int count)
            {
                if (dest == ItemDestination.Cold)
                {
                    coldCount = count;
                }
                else if (dest == ItemDestination.Warm)
                {
                    warmCount = count;
                }
            }
        }

        private void Cycle(int hotCount)
        {
            if (isWarm)
            {
                (var dest, var count) = CycleHot(hotCount);

                int cycles = 0;
                while (cycles++ < 3 && dest != ItemDestination.Remove)
                {
                    if (dest == ItemDestination.Warm)
                    {
                        (dest, count) = CycleWarm(count);
                    }
                    else if (dest == ItemDestination.Cold)
                    {
                        (dest, count) = CycleCold(count);
                    }
                }

                // If nothing was removed yet, constrain the size of warm and cold by discarding the coldest item.
                if (dest != ItemDestination.Remove)
                {
                    if (dest == ItemDestination.Warm && count > this.capacity.Warm)
                    {
                        count = LastWarmToCold();
                    }

                    ConstrainCold(count, ItemRemovedReason.Evicted);
                }
            }
            else
            {
                // fill up the warm queue with new items until warm is full.
                // else during warmup the cache will only use the hot + cold queues until any item is requested twice.
                CycleDuringWarmup(hotCount);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void CycleDuringWarmup(int hotCount)
        {
            // do nothing until hot is full
            if (hotCount > this.capacity.Hot)
            {
                Interlocked.Decrement(ref this.counter.hot);

                if (this.hotQueue.TryDequeue(out var item))
                {
                    int count = this.Move(item, ItemDestination.Warm, ItemRemovedReason.Evicted);

                    // if warm is now full, overflow to cold and mark as warm
                    if (count > this.capacity.Warm)
                    {
                        Volatile.Write(ref this.isWarm, true);
                        count = LastWarmToCold();
                        ConstrainCold(count, ItemRemovedReason.Evicted);
                    }
                }
                else
                {
                    Interlocked.Increment(ref this.counter.hot);
                }
            }
        }

        private (ItemDestination, int) CycleHot(int hotCount)
        {
            if (hotCount > this.capacity.Hot)
            {
                return CycleHotUnchecked(ItemRemovedReason.Evicted);
            }

            return (ItemDestination.Remove, 0);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private (ItemDestination, int) CycleHotUnchecked(ItemRemovedReason removedReason)
        {
            Interlocked.Decrement(ref this.counter.hot);

            if (this.hotQueue.TryDequeue(out var item))
            {
                var where = this.itemPolicy.RouteHot(item);
                return (where, this.Move(item, where, removedReason));
            }
            else
            {
                Interlocked.Increment(ref this.counter.hot);
                return (ItemDestination.Remove, 0);
            }
        }

        private (ItemDestination, int) CycleWarm(int count)
        {
            if (count > this.capacity.Warm)
            {
                return CycleWarmUnchecked(ItemRemovedReason.Evicted);
            }

            return (ItemDestination.Remove, 0);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private (ItemDestination, int) CycleWarmUnchecked(ItemRemovedReason removedReason)
        {
            int wc = Interlocked.Decrement(ref this.counter.warm);

            if (this.warmQueue.TryDequeue(out var item))
            {
                var where = this.itemPolicy.RouteWarm(item);

                // When the warm queue is full, we allow an overflow of 1 item before redirecting warm items to cold.
                // This only happens when hit rate is high, in which case we can consider all items relatively equal in
                // terms of which was least recently used.
                if (where == ItemDestination.Warm && wc <= this.capacity.Warm)
                {
                    return (ItemDestination.Warm, this.Move(item, where, removedReason));
                }
                else
                {
                    return (ItemDestination.Cold, this.Move(item, ItemDestination.Cold, removedReason));
                }
            }
            else
            {
                Interlocked.Increment(ref this.counter.warm);
                return (ItemDestination.Remove, 0);
            }
        }

        private (ItemDestination, int) CycleCold(int count)
        {
            if (count > this.capacity.Cold)
            {
                return TryRemoveCold(ItemRemovedReason.Evicted);
            }

            return (ItemDestination.Remove, 0);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private (ItemDestination, int) TryRemoveCold(ItemRemovedReason removedReason)
        {
            Interlocked.Decrement(ref this.counter.cold);

            if (this.coldQueue.TryDequeue(out var item))
            {
                var where = this.itemPolicy.RouteCold(item);

                if (where == ItemDestination.Warm && Volatile.Read(ref this.counter.warm) <= this.capacity.Warm)
                {
                    return (ItemDestination.Warm, this.Move(item, where, removedReason));
                }
                else
                {
                    this.Move(item, ItemDestination.Remove, removedReason);
                    return (ItemDestination.Remove, 0);
                }
            }
            else
            {
                return (ItemDestination.Cold, Interlocked.Increment(ref this.counter.cold));
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int LastWarmToCold()
        {
            Interlocked.Decrement(ref this.counter.warm);

            if (this.warmQueue.TryDequeue(out var item))
            {
                return this.Move(item, ItemDestination.Cold, ItemRemovedReason.Evicted);
            }
            else
            {
                Interlocked.Increment(ref this.counter.warm);
                return 0;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ConstrainCold(int coldCount, ItemRemovedReason removedReason)
        {
            if (coldCount > this.capacity.Cold && this.coldQueue.TryDequeue(out var item))
            {
                Interlocked.Decrement(ref this.counter.cold);
                this.Move(item, ItemDestination.Remove, removedReason);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int Move(I item, ItemDestination where, ItemRemovedReason removedReason)
        {
            item.WasAccessed = false;

            switch (where)
            {
                case ItemDestination.Warm:
                    this.warmQueue.Enqueue(item);
                    return Interlocked.Increment(ref this.counter.warm);
                case ItemDestination.Cold:
                    this.coldQueue.Enqueue(item);
                    return Interlocked.Increment(ref this.counter.cold);
                case ItemDestination.Remove:

                    var kvp = new KeyValuePair<K, I>(item.Key, item);

#if NET6_0_OR_GREATER
                    if (this.dictionary.TryRemove(kvp))
#else
                    // https://devblogs.microsoft.com/pfxteam/little-known-gems-atomic-conditional-removals-from-concurrentdictionary/
                    if (((ICollection<KeyValuePair<K, I>>)this.dictionary).Remove(kvp))
#endif
                    {
                        OnRemove(item.Key, item, removedReason);
                    }
                    break;
            }

            return 0;
        }

        /// <summary>Returns an enumerator that iterates through the cache.</summary>
        /// <returns>An enumerator for the cache.</returns>
        /// <remarks>
        /// The enumerator returned from the cache is safe to use concurrently with
        /// reads and writes, however it does not represent a moment-in-time snapshot.  
        /// The contents exposed through the enumerator may contain modifications
        /// made after <see cref="GetEnumerator"/> was called.
        /// </remarks>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((ConcurrentLruCore<K, V, I, P, T>)this).GetEnumerator();
        }

        private static CachePolicy CreatePolicy(ConcurrentLruCore<K, V, I, P, T> lru)
        {
            var p = new Proxy(lru);

            if (typeof(P) == typeof(AfterAccessPolicy<K, V>))
            {
                return new CachePolicy(new Optional<IBoundedPolicy>(p), Optional<ITimePolicy>.None(), new Optional<ITimePolicy>(p), Optional<IDiscreteTimePolicy>.None());
            }

            // IsAssignableFrom is a jit intrinsic https://github.com/dotnet/runtime/issues/4920
            if (typeof(IDiscreteItemPolicy<K, V>).IsAssignableFrom(typeof(P)))
            {
                return new CachePolicy(new Optional<IBoundedPolicy>(p), Optional<ITimePolicy>.None(), Optional<ITimePolicy>.None(), new Optional<IDiscreteTimePolicy>(new DiscreteExpiryProxy(lru)));
            }

            return new CachePolicy(new Optional<IBoundedPolicy>(p), lru.itemPolicy.CanDiscard() ? new Optional<ITimePolicy>(p) : Optional<ITimePolicy>.None());
        }

        private static Optional<ICacheMetrics> CreateMetrics(ConcurrentLruCore<K, V, I, P, T> lru)
        {
            if (typeof(T) == typeof(NoTelemetryPolicy<K, V>))
            {
                return Optional<ICacheMetrics>.None();
            }

            return new(new Proxy(lru));
        }

        private static Optional<ICacheEvents<K, V>> CreateEvents(ConcurrentLruCore<K, V, I, P, T> lru)
        {
            if (typeof(T) == typeof(NoTelemetryPolicy<K, V>))
            {
                return Optional<ICacheEvents<K, V>>.None();
            }

            return new(new Proxy(lru));
        }

        // To get JIT optimizations, policies must be structs.
        // If the structs are returned directly via properties, they will be copied. Since  
        // telemetryPolicy is a mutable struct, copy is bad. One workaround is to store the 
        // state within the struct in an object. Since the struct points to the same object
        // it becomes immutable. However, this object is then somewhere else on the 
        // heap, which slows down the policies with hit counter logic in benchmarks. Likely
        // this approach keeps the structs data members in the same CPU cache line as the LRU.
        // backcompat: remove conditional compile
#if NETCOREAPP3_0_OR_GREATER
        [DebuggerDisplay("Hit = {Hits}, Miss = {Misses}, Upd = {Updated}, Evict = {Evicted}")]
#else
        [DebuggerDisplay("Hit = {Hits}, Miss = {Misses}, Evict = {Evicted}")]
#endif
        private class Proxy : ICacheMetrics, ICacheEvents<K, V>, IBoundedPolicy, ITimePolicy
        {
            private readonly ConcurrentLruCore<K, V, I, P, T> lru;

            public Proxy(ConcurrentLruCore<K, V, I, P, T> lru)
            {
                this.lru = lru;
            }

            public double HitRatio => lru.telemetryPolicy.HitRatio;

            public long Total => lru.telemetryPolicy.Total;

            public long Hits => lru.telemetryPolicy.Hits;

            public long Misses => lru.telemetryPolicy.Misses;

            public long Evicted => lru.telemetryPolicy.Evicted;

// backcompat: remove conditional compile
#if NETCOREAPP3_0_OR_GREATER
            public long Updated => lru.telemetryPolicy.Updated;
#endif
            public int Capacity => lru.Capacity;

            public TimeSpan TimeToLive => lru.itemPolicy.TimeToLive;

            public event EventHandler<ItemRemovedEventArgs<K, V>> ItemRemoved
            {
                add { this.lru.telemetryPolicy.ItemRemoved += value; }
                remove { this.lru.telemetryPolicy.ItemRemoved -= value; }
            }

// backcompat: remove conditional compile
#if NETCOREAPP3_0_OR_GREATER
            public event EventHandler<ItemUpdatedEventArgs<K, V>> ItemUpdated
            {
                add { this.lru.telemetryPolicy.ItemUpdated += value; }
                remove { this.lru.telemetryPolicy.ItemUpdated -= value; }
            }
#endif
            public void Trim(int itemCount)
            {
                lru.Trim(itemCount);
            }

            public void TrimExpired()
            {
                lru.TrimExpired();
            }
        }

        private class DiscreteExpiryProxy : IDiscreteTimePolicy
        {
            private readonly ConcurrentLruCore<K, V, I, P, T> lru;

            public DiscreteExpiryProxy(ConcurrentLruCore<K, V, I, P, T> lru)
            {
                this.lru = lru;
            }

            public void TrimExpired()
            {
                lru.TrimExpired();
            }

            public bool TryGetTimeToExpire<TKey>(TKey key, out TimeSpan timeToLive)
            {
                if (key is K k && lru.dictionary.TryGetValue(k, out var item))
                {
                    LongTickCountLruItem<K, V>? tickItem = item as LongTickCountLruItem<K, V>;
                    timeToLive = (new Duration(tickItem!.TickCount) - Duration.SinceEpoch()).ToTimeSpan();
                    return true;
                }

                timeToLive = default;
                return false;
            }
        }
    }
}
