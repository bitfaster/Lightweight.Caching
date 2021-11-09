﻿using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using BitFaster.Caching.Lru;

namespace BitFaster.Caching.Lazy
{
    // Wrappers needed:
    // - Atomic
    // - Scoped (already exists)
    // - ScopedAtomic
    // - AsyncAtomic
    // - ScopedAsyncAtomic
    // There is no ScopedAsync, since that is just Scoped - the task is not stored so we only need scoped values in the cache.
    public class DesiredApi
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

        public static void HowToCacheScopedAtomic()
        {
            // ICache<K, ScopedAtomic<K, V>>
            var scopedAtomicLru = new ConcurrentLru<int, ScopedAtomic<int, SomeDisposable>>(5);

            using (var l = scopedAtomicLru.GetOrAdd(1, k => new SomeDisposable()))
            {
                SomeDisposable d = l.Value;
            }

            scopedAtomicLru.TryUpdate(2, new SomeDisposable());
            scopedAtomicLru.TryGet(1, out AtomicLifetime<int, SomeDisposable> v);
            scopedAtomicLru.AddOrUpdate(1, new SomeDisposable());
        }

        public async static Task HowToCacheAsyncAtomic()
        { 
            var asyncAtomicLru = new ConcurrentLru<int, AsyncAtomic<int, int>>(5);

            int ar = await asyncAtomicLru.GetOrAddAsync(1, i => Task.FromResult(i));

            asyncAtomicLru.TryUpdate(2, 3);
            asyncAtomicLru.TryGet(1, out int v);
            asyncAtomicLru.AddOrUpdate(1, 2);
        }

        // Requirements:
        // 1. lifetime/value create is async end to end (if async delegate is used to create value)
        // 2. value is created lazily, guarantee single instance of object, single invocation of lazy
        // 3. lazy value is disposed by scope
        // 4. lifetime keeps scope alive

        public static async Task HowToCacheScopedAsyncAtomic()
        {
            var scopedAsyncAtomicLru = new ConcurrentLru<int, ScopedAsyncAtomic<int, SomeDisposable>>(4);
            Func<int, Task<SomeDisposable>> valueFactory = k => Task.FromResult(new SomeDisposable());

            using (var lifetime = await scopedAsyncAtomicLru.GetOrAddAsync(1, valueFactory))
            {
                SomeDisposable y = lifetime.Value;
            }

            //scopedAsyncAtomicLru.TryUpdate(2, 3);
            //scopedAsyncAtomicLru.TryGet(1, out int v);
            //scopedAsyncAtomicLru.AddOrUpdate(1, 2);
        }
    }

    public class SomeDisposable : IDisposable
    {
        public void Dispose()
        {

        }
    }
}
