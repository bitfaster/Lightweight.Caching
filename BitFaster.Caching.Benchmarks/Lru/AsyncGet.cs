﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using BitFaster.Caching.Lru;

namespace BitFaster.Caching.Benchmarks.Lru
{
    [SimpleJob(RuntimeMoniker.Net48)]
    [SimpleJob(RuntimeMoniker.Net60)]
    [DisassemblyDiagnoser(printSource: true, maxDepth: 5)]
    [MemoryDiagnoser]
    public class AsyncGet
    {
        private static readonly IAsyncCache<int, string> concurrentLru = new ConcurrentLruBuilder<int, string>().AsAsyncCache().Build();
        private static readonly IAsyncCache<int, string> atomicConcurrentLru = new ConcurrentLruBuilder<int, string>().AsAsyncCache().WithAtomicCreate().Build();

        private static Task<string> returnTask = Task.FromResult("1");

        [Benchmark()]
        public async ValueTask<string> GetOrAddAsync()
        {
            Func<int, Task<string>> func = x => returnTask;

            return await concurrentLru.GetOrAddAsync(1, func);
        }

        [Benchmark()]
        public async ValueTask<string> AtomicGetOrAddAsync()
        {
            Func<int, Task<string>> func = x => returnTask;

            return await atomicConcurrentLru.GetOrAddAsync(1, func);
        }
    }
}
