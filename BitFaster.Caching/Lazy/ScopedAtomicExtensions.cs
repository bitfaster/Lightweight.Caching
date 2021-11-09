﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BitFaster.Caching.Lazy
{
    public static class ScopedAtomicExtensions
    {
        public static AtomicLifetime<K, V> GetOrAdd<K, V>(this ICache<K, ScopedAtomic<K, V>> cache, K key, Func<K, V> valueFactory) where V : IDisposable
        {
            while (true)
            {
                var scope = cache.GetOrAdd(key, _ => new ScopedAtomic<K, V>());

                if (scope.TryCreateLifetime(key, valueFactory, out var lifetime))
                {
                    return lifetime;
                }

                // How to make atomic lifetime a single alloc?
                //return scope.CreateLifetime(key, valueFactory);
            }
        }

        public static void AddOrUpdate<K, V>(this ICache<K, ScopedAtomic<K, V>> cache, K key, V value) where V : IDisposable
        {
            throw new NotImplementedException();
            //cache.AddOrUpdate(key, new ScopedAtomic<K, V>(value));
        }

        public static bool TryUpdate<K, V>(this ICache<K, ScopedAtomic<K, V>> cache, K key, V value) where V : IDisposable
        {
            throw new NotImplementedException();
            //return cache.TryUpdate(key, new Atomic<K, V>(value));
        }

        public static bool TryGet<K, V>(this ICache<K, ScopedAtomic<K, V>> cache, K key, out AtomicLifetime<K, V> value) where V : IDisposable
        {
            throw new NotImplementedException();

            ScopedAtomic<K, V> output;
            bool ret = cache.TryGet(key, out output);

            if (ret)
            {
                // TODO: only create a lifetime if the value exists, 
                //value = output.CreateLifetime(;
            }
            else
            {
                value = default;
            }

            //return ret;
        }
    }
}
