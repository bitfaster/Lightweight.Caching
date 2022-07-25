﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BitFaster.Caching.Atomic;

namespace BitFaster.Caching.Lru.Builder
{
    public class AtomicScopedConcurrentLruBuilder<K, V> : LruBuilderBase<K, V, AtomicScopedConcurrentLruBuilder<K, V>, IScopedCache<K, V>> where V : IDisposable
    {
        private readonly ConcurrentLruBuilder<K, ScopedAtomicFactory<K, V>> inner;

        internal AtomicScopedConcurrentLruBuilder(ConcurrentLruBuilder<K, ScopedAtomicFactory<K, V>> inner)
            : base(inner.info)
        {
            this.inner = inner;
        }

        public override IScopedCache<K, V> Build()
        {
            var level1 = inner.Build() as ICache<K, ScopedAtomicFactory<K, V>>;
            return new AtomicFactoryScopedCache<K, V>(level1);
        }
    }
}
