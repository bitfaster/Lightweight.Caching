﻿using System;
using System.Diagnostics;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;

namespace BitFaster.Caching.Benchmarks
{
   // [SimpleJob(RuntimeMoniker.Net48)]
    [SimpleJob(RuntimeMoniker.Net60)]
    [HideColumns("Job", "Median", "RatioSD", "Alloc Ratio")]
    public class TimeBenchmarks
    {
        private static readonly Stopwatch sw = Stopwatch.StartNew();

        [Benchmark(Baseline = true)]
        public DateTime DateTimeUtcNow()
        {
            return DateTime.UtcNow;
        }

        [Benchmark()]
        public int EnvironmentTickCount()
        {
            return Environment.TickCount;
        }
#if NET6_0_OR_GREATER
        [Benchmark()]
        public long EnvironmentTickCount64()
        {
            return Environment.TickCount64;
        }
#endif
        [Benchmark()]
        public long StopWatchGetElapsed()
        {
            return sw.ElapsedTicks;
        }
    }
}
