﻿using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using BitFaster.Caching.Lru;

namespace BitFaster.Caching.Lazy
{
    class DesiredApi
    {
        public static void HowToCacheAtomic()
        { 
            var lru = new ConcurrentLru<int, Atomic<int, int>>(4);

            // raw, this is a bit of a mess
            int r = lru.GetOrAdd(1, i => new Atomic<int, int>()).GetValue(1, x => x);

            // extension cleanup can hide it
            int rr = lru.GetOrAdd(1, i => i);

            lru.TryUpdate(2, 3);
            lru.TryGet(1, out int v);
            lru.AddOrUpdate(1, 2);
        }

        public async static Task HowToCacheAsyncAtomic()
        { 
            var asyncAtomicLru = new ConcurrentLru<int, AsyncAtomic<int, int>>(5);

            int ar = await asyncAtomicLru.GetOrAddAsync(1, i => Task.FromResult(i));

            asyncAtomicLru.TryUpdate(2, 3);
            asyncAtomicLru.TryGet(1, out int v);
            asyncAtomicLru.AddOrUpdate(1, 2);
        }

        public static void HowToCacheDisposableAtomic()
        {
            var scopedAtomicLru2 = new ConcurrentLru<int, ScopedAtomic<int, SomeDisposable>>(5);

            using (var l = scopedAtomicLru2.GetOrAdd(1, k => new SomeDisposable()))
            {
                SomeDisposable d = l.Value;
            }
        }

        // Requirements:
        // 1. lifetime/value create is async end to end (if async delegate is used to create value)
        // 2. value is created lazily, guarantee single instance of object, single invocation of lazy
        // 3. lazy value is disposed by scope
        // 4. lifetime keeps scope alive
#if NETCOREAPP3_1_OR_GREATER
        public static async Task HowToCacheADisposableAsyncLazy()
        {
            var lru = new ConcurrentLru<int, ScopedAtomicAsync<int, SomeDisposable>>(4);
            var factory = new ScopedAtomicAsyncFactory();

            //await using (var lifetime = await lru.GetOrAdd(1, factory.Create).CreateLifetimeAsync())
            //{
            //    // This is cleaned up by the magic GetAwaiter method
            //    SomeDisposable y = await lifetime.Task;
            //}
        }
#endif
    }

    //public class ScopedAtomicFactory
    //{
    //    public Task<SomeDisposable> CreateAsync(int key)
    //    {
    //        return Task.FromResult(new SomeDisposable());
    //    }

    //    public ScopedAtomic<SomeDisposable> Create(int key)
    //    {
    //        return new ScopedAtomic<SomeDisposable>(() => new SomeDisposable());
    //    }
    //}

#if NETCOREAPP3_1_OR_GREATER
    public class ScopedAtomicAsyncFactory
    {
        public Task<SomeDisposable> CreateAsync(int key)
        {
            return Task.FromResult(new SomeDisposable());
        }

        public ScopedAtomicAsync<int, SomeDisposable> Create(int key)
        {
            return new ScopedAtomicAsync<int, SomeDisposable>();
        }
    }
#endif

    public class SomeDisposable : IDisposable
    {
        public void Dispose()
        {

        }
    }

    //public static class AtomicCacheExtensions
    //{ 
    //    public static V GetOrAdd<K, V>(this ICache<K, Atomic<V>> cache, K key, Func<K, V> valueFactory)
    //    { 
    //        return cache.GetOrAdd(key, k => new Atomic<V>(() => valueFactory(k))).Value;
    //    }

    //    public static async Task<V> GetOrAddAsync<K, V>(this ICache<K, Atomic<V>> cache, K key, Func<K, V> valueFactory)
    //    { 
    //        var atomic = await cache.GetOrAddAsync(key, k => Task.FromResult(new Atomic<V>(() => valueFactory(k)))).ConfigureAwait(false);
    //        return atomic.Value;
    //    }
    //}
}
