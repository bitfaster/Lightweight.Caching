﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BitFaster.Caching
{
    /// <summary>
    /// A cache decorator for working with Scoped IDisposable values. The Scoped methods (e.g. ScopedGetOrAdd)
    /// are threadsafe and create lifetimes that guarantee the value will not be disposed until the
    /// lifetime is disposed.
    /// </summary>
    /// <typeparam name="K">The type of keys in the cache.</typeparam>
    /// <typeparam name="V">The type of values in the cache.</typeparam>
    public sealed class ScopedCache<K, V> : IScopedCache<K, V> where V : IDisposable
    {
        private readonly ICache<K, Scoped<V>> cache;

        private const int MaxRetry = 5;
        private static readonly string RetryFailureMessage = $"Exceeded {MaxRetry} attempts to create a lifetime.";

        public ScopedCache(ICache<K, Scoped<V>> cache)
        {
            if (cache == null)
            {
                throw new ArgumentNullException(nameof(cache));
            }

            this.cache = cache;
        }

        ///<inheritdoc/>
        public int Capacity => this.cache.Capacity;

        ///<inheritdoc/>
        public int Count => this.cache.Count;

        ///<inheritdoc/>
        public ICacheMetrics Metrics => this.cache.Metrics;

        ///<inheritdoc/>
        public void AddOrUpdate(K key, V value)
        {
            this.cache.AddOrUpdate(key, new Scoped<V>(value));
        }

        ///<inheritdoc/>
        public void Clear()
        {
            this.cache.Clear();
        }

        ///<inheritdoc/>
        public Lifetime<V> ScopedGetOrAdd(K key, Func<K, Scoped<V>> valueFactory)
        {
            int c = 0;
            var spinwait = new SpinWait();
            while (true)
            {
                var scope = cache.GetOrAdd(key, k => valueFactory(k));

                if (scope.TryCreateLifetime(out var lifetime))
                {
                    return lifetime;
                }

                spinwait.SpinOnce();

                if (c++ > MaxRetry)
                {
                    throw new InvalidOperationException(RetryFailureMessage);
                }
            }
        }

        ///<inheritdoc/>
        public async Task<Lifetime<V>> ScopedGetOrAddAsync(K key, Func<K, Task<Scoped<V>>> valueFactory)
        {
            int c = 0;
            var spinwait = new SpinWait();
            while (true)
            {
                var scope = await cache.GetOrAddAsync(key, valueFactory);

                if (scope.TryCreateLifetime(out var lifetime))
                {
                    return lifetime;
                }

                spinwait.SpinOnce();

                if (c++ > MaxRetry)
                {
                    throw new InvalidOperationException(RetryFailureMessage);
                }
            }
        }

        ///<inheritdoc/>
        public void Trim(int itemCount)
        {
            this.cache.Trim(itemCount);
        }

        ///<inheritdoc/>
        public bool ScopedTryGet(K key, out Lifetime<V> lifetime)
        {
            if (this.cache.TryGet(key, out var scope))
            {
                if (scope.TryCreateLifetime(out lifetime))
                {
                    return true;
                }
            }

            lifetime = default;
            return false;
        }

        ///<inheritdoc/>
        public bool TryRemove(K key)
        {
            return this.cache.TryRemove(key);
        }

        ///<inheritdoc/>
        public bool TryUpdate(K key, V value)
        {
            return this.cache.TryUpdate(key, new Scoped<V>(value));
        }
    }
}