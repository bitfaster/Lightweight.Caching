﻿
using BitFaster.Caching.Lru.Builder;

namespace BitFaster.Caching.Lru
{
    /// <summary>
    /// A builder of ICache and IScopedCache instances with the following configuration
    /// settings:
    /// <list type="bullet">
    ///   <item><description>The maximum size.</description></item>
    ///   <item><description>The concurrency level.</description></item>
    ///   <item><description>The key comparer.</description></item>
    /// </list>
    /// The following features can be selected which change the underlying cache implementation: 
    /// <list type="bullet">
    ///   <item><description>Collect metrics (e.g. hit rate). Small perf penalty.</description></item>
    ///   <item><description>Time based expiration, measured since last write.</description></item>
    ///   <item><description>Time based expiration, measured since last read.</description></item>
    ///   <item><description>Scoped IDisposable values.</description></item>
    ///   <item><description>Atomic value factory.</description></item>
    /// </list>
    /// </summary>
    /// <typeparam name="K">The type of keys in the cache.</typeparam>
    /// <typeparam name="V">The type of values in the cache.</typeparam>
    public sealed class ConcurrentLruBuilder<K, V> : LruBuilderBase<K, V, ConcurrentLruBuilder<K, V>, ICache<K, V>>
    {
        /// <summary>
        /// Creates a ConcurrentLruBuilder. Chain method calls onto ConcurrentLruBuilder to configure the cache then call Build to create a cache instance.
        /// </summary>
        public ConcurrentLruBuilder()
            : base(new LruInfo<K>())
        {
        }

        internal ConcurrentLruBuilder(LruInfo<K> info)
            : base(info)
        {
        }

        /// <summary>
        /// Evict after a variable duration specified by an IExpiry instance.
        /// </summary>
        /// <param name="expiry">The expiry that determines item time to live.</param>
        /// <returns>A ConcurrentLruBuilder</returns>
        public ConcurrentLruBuilder<K, V> WithExpiry(IExpiry<K, V> expiry)
        {
            this.info.SetExpiry(expiry);
            return this;
        }

        ///<inheritdoc/>
        public override ICache<K, V> Build()
        {
            if (info.TimeToExpireAfterWrite.HasValue && info.TimeToExpireAfterAccess.HasValue)
                Throw.InvalidOp("Specifying both ExpireAfterWrite and ExpireAfterAccess is not supported.");

            var expiry = info.GetExpiry<V>();

            if (info.TimeToExpireAfterWrite.HasValue && expiry != null)
                Throw.InvalidOp("Specifying both ExpireAfterWrite and ExpireAfter is not supported.");

            if (info.TimeToExpireAfterAccess.HasValue && expiry != null)
                Throw.InvalidOp("Specifying both ExpireAfterAccess and ExpireAfter is not supported.");

            return info switch
            {
                LruInfo<K> i when i.WithMetrics && !i.TimeToExpireAfterWrite.HasValue && !i.TimeToExpireAfterAccess.HasValue => new ConcurrentLru<K, V>(info.ConcurrencyLevel, info.Capacity, info.KeyComparer),
                LruInfo<K> i when i.WithMetrics && i.TimeToExpireAfterWrite.HasValue && !i.TimeToExpireAfterAccess.HasValue => new ConcurrentTLru<K, V>(info.ConcurrencyLevel, info.Capacity, info.KeyComparer, info.TimeToExpireAfterWrite.Value),
                LruInfo<K> i when i.TimeToExpireAfterWrite.HasValue && !i.TimeToExpireAfterAccess.HasValue => new FastConcurrentTLru<K, V>(info.ConcurrencyLevel, info.Capacity, info.KeyComparer, info.TimeToExpireAfterWrite.Value),
                LruInfo<K> i when i.WithMetrics && !i.TimeToExpireAfterWrite.HasValue && i.TimeToExpireAfterAccess.HasValue => CreateExpireAfterAccess<TelemetryPolicy<K, V>>(info),
                LruInfo<K> i when !i.TimeToExpireAfterWrite.HasValue && i.TimeToExpireAfterAccess.HasValue => CreateExpireAfterAccess<NoTelemetryPolicy<K, V>>(info),
                LruInfo<K> i when i.WithMetrics && expiry != null => CreateExpireAfter<TelemetryPolicy<K, V>>(info, expiry),
                LruInfo<K> _ when expiry != null => CreateExpireAfter<NoTelemetryPolicy<K, V>>(info, expiry),
                _ => new FastConcurrentLru<K, V>(info.ConcurrencyLevel, info.Capacity, info.KeyComparer),
            };
        }

        private static ICache<K, V> CreateExpireAfterAccess<TP>(LruInfo<K> info) where TP : struct, ITelemetryPolicy<K, V>
        {
            return new ConcurrentLruCore<K, V, LongTickCountLruItem<K, V>, AfterAccessLongTicksPolicy<K, V>, TP>(
                info.ConcurrencyLevel, info.Capacity, info.KeyComparer, new AfterAccessLongTicksPolicy<K, V>(info.TimeToExpireAfterAccess.Value), default);
        }

        private static ICache<K, V> CreateExpireAfter<TP>(LruInfo<K> info, IExpiry<K, V> expiry) where TP : struct, ITelemetryPolicy<K, V>
        {
            return new ConcurrentLruCore<K, V, LongTickCountLruItem<K, V>, CustomExpiryPolicy<K, V>, TP>(
                    info.ConcurrencyLevel, info.Capacity, info.KeyComparer, new CustomExpiryPolicy<K, V>(expiry), default);
            
        }
    }
}
