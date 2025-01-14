using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using BitFaster.Caching.Buffers;
using BitFaster.Caching.Counters;
using BitFaster.Caching.Scheduler;

#if DEBUG
using System.Text;
#endif

namespace BitFaster.Caching.Lfu
{
    /// <summary>
    /// An approximate LFU based on the W-TinyLfu eviction policy. W-TinyLfu tracks items using a window LRU list, and 
    /// a main space LRU divided into protected and probation segments. Reads and writes to the cache are stored in buffers
    /// and later applied to the policy LRU lists in batches under a lock. Each read and write is tracked using a compact 
    /// popularity sketch to probalistically estimate item frequency. Items proceed through the LRU lists as follows:
    /// <list type="number">
    ///   <item><description>New items are added to the window LRU. When acessed window items move to the window MRU position.</description></item>
    ///   <item><description>When the window is full, candidate items are moved to the probation segment in LRU order.</description></item>
    ///   <item><description>When the main space is full, the access frequency of each window candidate is compared 
    ///   to probation victims in LRU order. The item with the lowest frequency is evicted until the cache size is within bounds.</description></item>
    ///   <item><description>When a probation item is accessed, it is moved to the protected segment. If the protected segment is full, 
    ///   the LRU protected item is demoted to probation.</description></item>
    ///   <item><description>When a protected item is accessed, it is moved to the protected MRU position.</description></item>
    /// </list>
    /// The size of the admission window and main space are adapted over time to iteratively improve hit rate using a 
    /// hill climbing algorithm. A larger window favors workloads with high recency bias, whereas a larger main space
    /// favors workloads with frequency bias.
    /// </summary>
    /// Based on the Caffeine library by ben.manes@gmail.com (Ben Manes).
    /// https://github.com/ben-manes/caffeine
    
    internal struct ConcurrentLfuCore<K, V, N, P> : IBoundedPolicy
        where K : notnull
        where N : LfuNode<K, V>
        where P : struct, INodePolicy<K, V, N>
    {
        private const int MaxWriteBufferRetries = 64;

        private const int DefaultBufferSize = 128;

        private readonly ConcurrentDictionary<K, N> dictionary;

        internal readonly StripedMpscBuffer<N> readBuffer;
        internal readonly MpscBoundedBuffer<N> writeBuffer;

        private readonly CacheMetrics metrics = new();

        private readonly CmSketch<K> cmSketch;

        private readonly LfuNodeList<K, V> windowLru;
        private readonly LfuNodeList<K, V> probationLru;
        private readonly LfuNodeList<K, V> protectedLru;

        private readonly LfuCapacityPartition capacity;

        internal readonly DrainStatus drainStatus = new();

#if NET9_0_OR_GREATER
        private readonly Lock maintenanceLock = new();
#else
        private readonly object maintenanceLock = new();
#endif
        private readonly IScheduler scheduler;
        private readonly Action drainBuffers;

        private readonly N[] drainBuffer;

        internal P policy;

        public ConcurrentLfuCore(int concurrencyLevel, int capacity, IScheduler scheduler, IEqualityComparer<K> comparer, Action drainBuffers, P policy)
        {
            if (capacity < 3)
                Throw.ArgOutOfRange(nameof(capacity));

            int dictionaryCapacity = ConcurrentDictionarySize.Estimate(capacity);
            this.dictionary = new (concurrencyLevel, dictionaryCapacity, comparer);

            // cap concurrency at proc count * 2
            int readStripes = Math.Min(BitOps.CeilingPowerOfTwo(concurrencyLevel), BitOps.CeilingPowerOfTwo(Environment.ProcessorCount * 2));
            this.readBuffer = new (readStripes, DefaultBufferSize);

            // Cap the write buffer to the cache size, or 128. Whichever is smaller.
            int writeBufferSize = Math.Min(BitOps.CeilingPowerOfTwo(capacity), 128);
            this.writeBuffer = new (writeBufferSize);

            this.cmSketch = new CmSketch<K>(capacity, comparer);
            this.windowLru = new LfuNodeList<K, V>();
            this.probationLru = new LfuNodeList<K, V>();
            this.protectedLru = new LfuNodeList<K, V>();

            this.capacity = new LfuCapacityPartition(capacity);

            this.scheduler = scheduler;

            this.drainBuffer = new N[this.readBuffer.Capacity];

            this.drainBuffers = drainBuffers;

            this.policy = policy;
        }

        // No lock count: https://arbel.net/2013/02/03/best-practices-for-using-concurrentdictionary/
        public int Count => this.dictionary.Skip(0).Count();

        public int Capacity => this.capacity.Capacity;

        public Optional<ICacheMetrics> Metrics => new(this.metrics);

        public CachePolicy Policy => new(new Optional<IBoundedPolicy>(this), Optional<ITimePolicy>.None());

        public ICollection<K> Keys => this.dictionary.Keys;

        public IScheduler Scheduler => scheduler;

        public void AddOrUpdate(K key, V value)
        {
            while (true)
            {
                if (TryUpdate(key, value))
                {
                    return;
                }

                var node = policy.Create(key, value);
                if (this.dictionary.TryAdd(key, node))
                {
                    AfterWrite(node);
                    return;
                }
            }
        }

        public void Clear()
        {
            Trim(int.MaxValue);

            lock (maintenanceLock)
            {
                this.readBuffer.Clear();
                this.writeBuffer.Clear();
                this.cmSketch.Clear();
            }
        }

        public void Trim(int itemCount)
        {
            List<LfuNode<K, V>> candidates;
            lock (maintenanceLock)
            {
                Maintenance();

                int lruCount = this.windowLru.Count + this.probationLru.Count + this.protectedLru.Count;
                itemCount = Math.Min(itemCount, lruCount);
                candidates = new (itemCount);

                // Note: this is LRU order eviction, Caffeine is based on frequency
                // walk in lru order, get itemCount keys to evict
                TakeCandidatesInLruOrder(this.probationLru, candidates, itemCount);
                TakeCandidatesInLruOrder(this.protectedLru, candidates, itemCount);
                TakeCandidatesInLruOrder(this.windowLru, candidates, itemCount);
            }

#if NET6_0_OR_GREATER
            foreach (var candidate in CollectionsMarshal.AsSpan(candidates))
#else
            foreach (var candidate in candidates)
#endif
            {
                this.TryRemove(candidate.Key);
            }
        }

        private bool TryAdd(K key, V value)
        {
            var node = policy.Create(key, value);

            if (this.dictionary.TryAdd(key, node))
            {
                AfterWrite(node);
                return true;
            }

            Disposer<V>.Dispose(node.Value);
            return false;
        }

        public V GetOrAdd(K key, Func<K, V> valueFactory)
        {
            while (true)
            {
                if (this.TryGet(key, out V? value))
                {
                    return value;
                }

                value = valueFactory(key);
                if (this.TryAdd(key, value))
                {
                    return value;
                }
            }
        }

        public V GetOrAdd<TArg>(K key, Func<K, TArg, V> valueFactory, TArg factoryArgument)
        {
            while (true)
            {
                if (this.TryGet(key, out V? value))
                {
                    return value;
                }

                value = valueFactory(key, factoryArgument);
                if (this.TryAdd(key, value))
                {
                    return value;
                }
            }
        }

        public async ValueTask<V> GetOrAddAsync(K key, Func<K, Task<V>> valueFactory)
        {
            while (true)
            {
                if (this.TryGet(key, out V? value))
                {
                    return value;
                }

                value = await valueFactory(key).ConfigureAwait(false);
                if (this.TryAdd(key, value))
                {
                    return value;
                }
            }
        }

        public async ValueTask<V> GetOrAddAsync<TArg>(K key, Func<K, TArg, Task<V>> valueFactory, TArg factoryArgument)
        {
            while (true)
            {
                if (this.TryGet(key, out V? value))
                {
                    return value;
                }

                value = await valueFactory(key, factoryArgument).ConfigureAwait(false);
                if (this.TryAdd(key, value))
                {
                    return value;
                }
            }
        }

        public bool TryGet(K key, [MaybeNullWhen(false)] out V value)
        {
            return TryGetImpl(key, out value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TryGetImpl(K key, [MaybeNullWhen(false)] out V value)
        {
            if (this.dictionary.TryGetValue(key, out var node))
            {
                if (!policy.IsExpired(node))
                {
                    bool delayable = this.readBuffer.TryAdd(node) != BufferStatus.Full;

                    if (this.drainStatus.ShouldDrain(delayable))
                    {
                        TryScheduleDrain();
                    }
                    this.policy.OnRead(node);
                    value = node.Value;
                    return true;
                }
                else
                {
                    // expired case, immediately remove from the dictionary
                    TryRemove(node);
                }
            }

            this.metrics.requestMissCount.Increment();

            value = default;
            return false;
        }

        internal bool TryGetNode(K key, [MaybeNullWhen(false)] out N node)
        {
            return this.dictionary.TryGetValue(key, out node);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        void TryRemove(N node)
        {
#if NET6_0_OR_GREATER
                if (this.dictionary.TryRemove(new KeyValuePair<K, N>(node.Key, node)))
#else
                // https://devblogs.microsoft.com/pfxteam/little-known-gems-atomic-conditional-removals-from-concurrentdictionary/
                if (((ICollection<KeyValuePair<K, N>>)this.dictionary).Remove(new KeyValuePair<K, N>(node.Key, node)))
#endif
            {
                node.WasRemoved = true;
                AfterWrite(node);
            }
        }

        public bool TryRemove(KeyValuePair<K, V> item)
        {
            if (this.dictionary.TryGetValue(item.Key, out var node))
            {
                lock (node)
                { 
                    if (EqualityComparer<V>.Default.Equals(node.Value, item.Value))
                    {
                        var kvp = new KeyValuePair<K, N>(item.Key, node);

#if NET6_0_OR_GREATER
                        if (this.dictionary.TryRemove(kvp))
#else
                        // https://devblogs.microsoft.com/pfxteam/little-known-gems-atomic-conditional-removals-from-concurrentdictionary/
                        if (((ICollection<KeyValuePair<K, N>>)this.dictionary).Remove(kvp))
#endif
                        {
                            node.WasRemoved = true;
                            AfterWrite(node);
                            return true;
                        }
                    }
                }
            }

            return false;
        }

        public bool TryRemove(K key, [MaybeNullWhen(false)] out V value)
        {
            if (this.dictionary.TryRemove(key, out var node))
            {
                node.WasRemoved = true;
                AfterWrite(node);
                value = node.Value;
                return true;
            }

            value = default;
            return false;
        }

        public bool TryRemove(K key)
        {
            return this.TryRemove(key, out var _);
        }

        public bool TryUpdate(K key, V value)
        {
            if (this.dictionary.TryGetValue(key, out var node))
            {
                lock (node)
                { 
                    if (!node.WasRemoved)
                    {
                         node.Value = value;

                        // It's ok for this to be lossy, since the node is already tracked
                        // and we will just lose ordering/hit count, but not orphan the node.
                        this.writeBuffer.TryAdd(node);
                        TryScheduleDrain();
                        this.policy.OnWrite(node);
                        return true;
                    }
                }
            }

            return false;
        }

        public void DoMaintenance()
        {
            DrainBuffers();
        }

        public IEnumerator<KeyValuePair<K, V>> GetEnumerator()
        {
            foreach (var kvp in this.dictionary)
            {
                yield return new KeyValuePair<K, V>(kvp.Key, kvp.Value.Value);
            }
        }

        private static void TakeCandidatesInLruOrder(LfuNodeList<K, V> lru, List<LfuNode<K, V>> candidates, int itemCount)
        {
            var curr = lru.First;

            while (candidates.Count < itemCount && curr != null)
            {
                // LRUs can contain items that are already removed, skip those 
                if (!curr.WasRemoved)
                { 
                    candidates.Add(curr); 
                }

                curr = curr.Next;
            }
        }

        private void AfterWrite(N node)
        {
            for (int i = 0; i < MaxWriteBufferRetries; i++)
            {
                if (writeBuffer.TryAdd(node) == BufferStatus.Success)
                {
                    ScheduleAfterWrite();
                    return;
                }

                TryScheduleDrain();
            }

            lock (this.maintenanceLock)
            {
                // aggressively try to exit the lock early before doing full maintenance
                var status = BufferStatus.Contended;
                while (status != BufferStatus.Full)
                {
                    status = writeBuffer.TryAdd(node);

                    if (status == BufferStatus.Success)
                    {
                        ScheduleAfterWrite();
                        return;
                    }
                }

                // if the write was dropped from the buffer, explicitly pass it to maintenance
                Maintenance(node);
            }
        }

        private void ScheduleAfterWrite()
        {
            var spinner = new SpinWait();
            int status = this.drainStatus.NonVolatileRead();
            while (true)
            {
                switch (status)
                {
                    case DrainStatus.Idle:
                        this.drainStatus.Cas(DrainStatus.Idle, DrainStatus.Required);
                        TryScheduleDrain();
                        return;
                    case DrainStatus.Required:
                        TryScheduleDrain();
                        return;
                    case DrainStatus.ProcessingToIdle:
                        if (this.drainStatus.Cas(DrainStatus.ProcessingToIdle, DrainStatus.ProcessingToRequired))
                        {
                            return;
                        }
                        status = this.drainStatus.VolatileRead();
                        break;
                    case DrainStatus.ProcessingToRequired:
                        return;
                }
                spinner.SpinOnce();
            }
        }

        private void TryScheduleDrain()
        {
            if (this.drainStatus.VolatileRead() >= DrainStatus.ProcessingToIdle)
            {
                return;
            }

#if NET9_0_OR_GREATER
            if (maintenanceLock.TryEnter())
#else
            bool lockTaken = false;
            Monitor.TryEnter(maintenanceLock, ref lockTaken);
            if (lockTaken)
#endif
            {
                try
                {
                    int status = this.drainStatus.NonVolatileRead();

                    if (status >= DrainStatus.ProcessingToIdle)
                    {
                        return;
                    }

                    this.drainStatus.VolatileWrite(DrainStatus.ProcessingToIdle);
                    scheduler.Run(this.drainBuffers);
                }
                finally
                {                
#if NET9_0_OR_GREATER
                    maintenanceLock.Exit();
#else
                    if (lockTaken)
                    {
                        Monitor.Exit(maintenanceLock);
                    }
#endif                                     
                }
            }
        }

        internal void DrainBuffers()
        {
            bool done = false;

            while (!done)
            {
                lock (maintenanceLock)
                {
                    done = Maintenance();
                }

                // don't run continuous foreground maintenance
                if (!scheduler.IsBackground)
                {
                    done = true;
                }
            }

            if (this.drainStatus.VolatileRead() == DrainStatus.Required)
            {
                TryScheduleDrain();
            }
        }

        private bool Maintenance(N? droppedWrite = null)
        {
            this.drainStatus.VolatileWrite(DrainStatus.ProcessingToIdle);

            // Note: this is only Span on .NET Core 3.1+, else this is no-op and it is still an array
            var buffer = this.drainBuffer.AsSpanOrArray();

            // extract to a buffer before doing book keeping work, ~2x faster
            int readCount = readBuffer.DrainTo(buffer);

            for (int i = 0; i < readCount; i++)
            {
                this.cmSketch.Increment(buffer[i].Key);
            }

            for (int i = 0; i < readCount; i++)
            {
                OnAccess(buffer[i]);
            }

            int writeCount = this.writeBuffer.DrainTo(buffer.AsSpanOrSegment());

            for (int i = 0; i < writeCount; i++)
            {
                OnWrite(buffer[i]);
            }

            // we are done only when both buffers are empty
            var done = readCount == 0 & writeCount == 0;

            if (droppedWrite != null)
            {
                OnWrite(droppedWrite);
                done = true;
            }

            policy.ExpireEntries(ref this);
            EvictEntries();
            this.capacity.OptimizePartitioning(this.metrics, this.cmSketch.ResetSampleSize);
            ReFitProtected();

            // Reset to idle if either
            // 1. We drained both input buffers (all work done)
            // 2. or scheduler is foreground (since don't run continuously on the foreground)
            if ((done || !scheduler.IsBackground) &&
                (this.drainStatus.NonVolatileRead() != DrainStatus.ProcessingToIdle ||
                !this.drainStatus.Cas(DrainStatus.ProcessingToIdle, DrainStatus.Idle)))
            {
                this.drainStatus.NonVolatileWrite(DrainStatus.Required);
            }

            return done;
        }

        private void OnAccess(N node)
        {
            // there was a cache hit even if the item was removed or is not yet added.
            this.metrics.requestHitCount++;

            // Node is added to read buffer while it is removed by maintenance, or it is read before it has been added.
            if (node.list == null)
            {
                return;
            }

            switch (node.Position)
            {
                case Position.Window:
                    this.windowLru.MoveToEnd(node); 
                    break;
                case Position.Probation:
                    PromoteProbation(node); 
                    break;
                case Position.Protected:
                    this.protectedLru.MoveToEnd(node);
                    break;
            }

            policy.AfterRead(node);
        }

        private void OnWrite(N node)
        {
            // Nodes can be removed while they are in the write buffer, in which case they should
            // not be added back into the LRU.
            if (node.WasRemoved)
            {
                node.list?.Remove(node);

                if (!node.WasDeleted)
                {
                    // if a write is in the buffer and is then removed in the buffer, it will enter OnWrite twice.
                    // we mark as deleted to avoid double counting/disposing it
                    this.metrics.evictedCount++;
                    Disposer<V>.Dispose(node.Value);
                    node.WasDeleted = true;
                }

                return;
            }

            this.cmSketch.Increment(node.Key);

            // node can already be in one of the queues due to update
            switch (node.Position)
            {
                case Position.Window:
                    if (node.list == null)
                    {
                        this.windowLru.AddLast(node);
                    }
                    else
                    {
                        this.windowLru.MoveToEnd(node);
                        this.metrics.updatedCount++;
                    }
                    break;
                case Position.Probation:
                    PromoteProbation(node);
                    this.metrics.updatedCount++;
                    break;
                case Position.Protected:
                    this.protectedLru.MoveToEnd(node);
                    this.metrics.updatedCount++;
                    break;
            }

            policy.AfterWrite(node);
        }

        private void PromoteProbation(LfuNode<K, V> node)
        {
            this.probationLru.Remove(node);
            this.protectedLru.AddLast(node);
            node.Position = Position.Protected;

            // If the protected space exceeds its maximum, the LRU items are demoted to the probation space.
            if (this.protectedLru.Count > this.capacity.Protected)
            {
                var demoted = this.protectedLru.First;
                this.protectedLru.RemoveFirst();

                demoted.Position = Position.Probation;
                this.probationLru.AddLast(demoted);
            }
        }

        private void EvictEntries()
        {
            var candidate = EvictFromWindow();
            EvictFromMain(candidate);
        }

        private LfuNode<K, V> EvictFromWindow()
        {
            LfuNode<K, V>? first = null;

            while (this.windowLru.Count > this.capacity.Window)
            {
                var node = this.windowLru.First;
                this.windowLru.RemoveFirst();

                first ??= node;

                this.probationLru.AddLast(node);
                node.Position = Position.Probation;
            }

            return first!;
        }

        private ref struct EvictIterator
        {
            private readonly CmSketch<K> sketch;
            public LfuNode<K, V> node;
            public int freq;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public EvictIterator(CmSketch<K> sketch, LfuNode<K, V> node)
            {
                this.sketch = sketch;
                this.node = node;
                freq = node == null ? -1 : sketch.EstimateFrequency(node.Key);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Next()
            {
                node = node.Next;

                if (node != null)
                {
                    freq = sketch.EstimateFrequency(node.Key);
                }
            }
        }

        private void EvictFromMain(LfuNode<K, V> candidateNode)
        {
            var victim = new EvictIterator(this.cmSketch, this.probationLru.First); // victims are LRU position in probation
            var candidate = new EvictIterator(this.cmSketch, candidateNode);

            // first pass: admit candidates
            while (this.windowLru.Count + this.probationLru.Count + this.protectedLru.Count > this.Capacity)
            {
                // bail when we run out of options
                if (candidate.node == null | victim.node == null)
                {
                    break;
                }

                if (victim.node == candidate.node)
                {
                    Evict(candidate.node!);
                    break;
                }

                if (candidate.node!.WasRemoved)
                {
                    var evictee = candidate.node;
                    candidate.Next();
                    Evict(evictee);
                    continue;
                }

                if (victim.node!.WasRemoved)
                {
                    var evictee = victim.node;
                    victim.Next();
                    Evict(evictee);
                    continue;
                }

                // Evict the entry with the lowest frequency
                if (candidate.freq > victim.freq)
                {
                    var evictee = victim.node;

                    // victim is initialized to first, and iterates forwards
                    victim.Next();
                    candidate.Next();

                    Evict(evictee);
                }
                else
                {
                    var evictee = candidate.node;

                    // candidate is initialized to first cand, and iterates forwards
                    candidate.Next();

                    Evict(evictee);
                }
            }

            // 2nd pass: remove probation items in LRU order, evict lowest frequency
            while (this.windowLru.Count + this.probationLru.Count + this.protectedLru.Count > this.Capacity)
            {
                var victim1 = this.probationLru.First;
                var victim2 = victim1.Next;

                if (AdmitCandidate(victim1.Key, victim2.Key))
                {
                    Evict(victim2);
                }
                else
                {
                    Evict(victim1);
                }
            }
        }

        private bool AdmitCandidate(K candidateKey, K victimKey)
        {
            int victimFreq = this.cmSketch.EstimateFrequency(victimKey);
            int candidateFreq = this.cmSketch.EstimateFrequency(candidateKey);

            // TODO: random factor when candidate freq < 5
            return candidateFreq > victimFreq;
        }

        internal void Evict(LfuNode<K, V> evictee)
        {
            evictee.WasRemoved = true;
            evictee.WasDeleted = true;

            // This handles the case where the same key exists in the write buffer both
            // as added and removed. Remove via KVP ensures we don't remove added nodes.
            var kvp = new KeyValuePair<K, N>(evictee.Key, (N)evictee);
#if NET6_0_OR_GREATER
            this.dictionary.TryRemove(kvp);
#else
            ((ICollection<KeyValuePair<K, N>>)this.dictionary).Remove(kvp);
#endif
            evictee.list?.Remove(evictee);
            Disposer<V>.Dispose(evictee.Value);
            this.metrics.evictedCount++;

            this.policy.OnEvict((N)evictee);
        }

        private void ReFitProtected()
        {
            // If hill climbing decreased protected, there may be too many items
            // - demote overflow to probation.
            while (this.protectedLru.Count > this.capacity.Protected)
            {
                var demoted = this.protectedLru.First;
                this.protectedLru.RemoveFirst();

                demoted.Position = Position.Probation;
                this.probationLru.AddLast(demoted);
            }
        }

        [DebuggerDisplay("{Format(),nq}")]
        internal class DrainStatus
        {
            public const int Idle = 0;
            public const int Required = 1;
            public const int ProcessingToIdle = 2;
            public const int ProcessingToRequired = 3;

            private PaddedInt drainStatus; // mutable struct, don't mark readonly

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool ShouldDrain(bool delayable)
            {
                int status = this.NonVolatileRead();
                return status switch
                {
                    Idle => !delayable,
                    Required => true,
                    // ProcessingToIdle or ProcessingToRequired => false, undefined not reachable
                    _ => false,
                };
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void VolatileWrite(int newStatus)
            { 
                Volatile.Write(ref this.drainStatus.Value, newStatus);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void NonVolatileWrite(int newStatus)
            {
                this.drainStatus.Value = newStatus;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool Cas(int oldStatus, int newStatus)
            { 
                return Interlocked.CompareExchange(ref this.drainStatus.Value, newStatus, oldStatus) == oldStatus;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int VolatileRead()
            {
                return Volatile.Read(ref this.drainStatus.Value);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int NonVolatileRead()
            {
                return this.drainStatus.Value;
            }

            [ExcludeFromCodeCoverage]
            internal string Format()
            {
                switch (this.drainStatus.Value)
                {
                    case Idle:
                        return "Idle";
                    case Required:
                        return "Required";
                    case ProcessingToIdle:
                        return "ProcessingToIdle";
                    case ProcessingToRequired:
                        return "ProcessingToRequired"; ;
                }

                return "Invalid state";
            }
        }

        [DebuggerDisplay("Hit = {Hits}, Miss = {Misses}, Upd = {Updated}, Evict = {Evicted}")]
        internal class CacheMetrics : ICacheMetrics
        {
            public long requestHitCount;
            public Counter requestMissCount = new();
            public long updatedCount;
            public long evictedCount;

            public double HitRatio => (double)requestHitCount / (double)Total;

            public long Total => requestHitCount + requestMissCount.Count();

            public long Hits => requestHitCount;

            public long Misses => requestMissCount.Count();

            public long Updated => updatedCount;

            public long Evicted => evictedCount;
        }

#if DEBUG
        /// <summary>
        /// Format the LFU as a string by converting all the keys to strings.
        /// </summary>
        /// <returns>The LFU formatted as a string.</returns>
        public string FormatLfuString()
        {
            var sb = new StringBuilder();

            sb.Append("W [");
            sb.Append(string.Join(",", this.windowLru.Select(n => n.Key.ToString())));
            sb.Append("] Protected [");
            sb.Append(string.Join(",", this.protectedLru.Select(n => n.Key.ToString())));
            sb.Append("] Probation [");
            sb.Append(string.Join(",", this.probationLru.Select(n => n.Key.ToString())));
            sb.Append(']');

            return sb.ToString();
        }
#endif
    }

    // Explicit layout cannot be a generic class member
    [StructLayout(LayoutKind.Explicit, Size = 2 * Padding.CACHE_LINE_SIZE)]
    internal struct PaddedInt
    {
        [FieldOffset(1 * Padding.CACHE_LINE_SIZE)] public int Value;
    }
}
