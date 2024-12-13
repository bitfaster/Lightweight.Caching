using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Perfolizer.Mathematics.OutlierDetection;

namespace BitFaster.Caching.ThroughputAnalysis
{
    // This is taken from BenchmarkDotNet:
    // https://github.com/dotnet/BenchmarkDotNet/blob/b4ac9df9f7890ca9669e2b9c8835af35c072a453/src/BenchmarkDotNet/Engines/DeadCodeEliminationHelper.cs#L6
    public static class DeadCodeEliminationHelper
    {
        [MethodImpl(MethodImplOptions.NoInlining)]
        public static void KeepAliveWithoutBoxing<T>(T _) { }
    }

    public interface IThroughputBenchmark
    {
        double Run(int warmup, int runs, int threads, IThroughputBenchConfig config, ICache<int, int> cache);
    }

    public abstract class ThroughputBenchmarkBase
    {
        public Action<ICache<long, int>> Initialize { get; set; }

        // https://github.com/dotnet/BenchmarkDotNet/blob/b4ac9df9f7890ca9669e2b9c8835af35c072a453/src/BenchmarkDotNet/Engines/EngineGeneralStage.cs#L18
        public (int, double) Run(int warmup, int runs, int threads, IThroughputBenchConfig config, ICache<long, int> cache)
        {
            var results = new List<double>();

            Initialize?.Invoke(cache);

            // Warmup a few times before estimating run time
            config.Iterations = 1;

            for (int i = 0; i < warmup; i++)
            {
                Run(Stage.Warmup, i, threads, config, cache);
            }

            // Pilot stage: estimate how many iterations to use to get stable measurements.
            // this gives a run time of about 30 seconds per benchmark with 80 runs per config
            int valid = 0;
            while (true)
            {
                var sw = Stopwatch.StartNew();
                Run(Stage.Pilot, 0, threads, config, cache);

                valid = sw.Elapsed > TimeSpan.FromMilliseconds(800) ? valid + 1 : 0;    

                if (valid > 3)
                {
                    break;
                }

                if (valid == 0)
                { 
                    config.Iterations = config.Iterations < 5 ? config.Iterations + 1 : (int)(1.2 * config.Iterations); 
                }
            }

            int runCounter = 0;
            double effectiveMaxRelativeError = 0.02; // https://github.com/dotnet/BenchmarkDotNet/blob/b4ac9df9f7890ca9669e2b9c8835af35c072a453/src/BenchmarkDotNet/Jobs/AccuracyMode.cs#L11

            OutlierMode outlierMode = OutlierMode.RemoveLower;
            int maxRuns = 80;

            while (true)
            {
                runCounter++;
                results.Add(Run(Stage.Workload, runCounter, threads, config, cache));
                var statistics = MeasurementsStatistics.Calculate(results, outlierMode);
                double actualError = statistics.ConfidenceInterval.Margin;

                double maxError1 = effectiveMaxRelativeError * statistics.Mean;
                double maxError2 = double.MaxValue;
                double maxError = Math.Min(maxError1, maxError2);

                if (runCounter >= runs && actualError < maxError)
                    break;

                if (runCounter >= maxRuns)
                    break;

                Console.Write("_");
            }

            Console.WriteLine();

            var finalStats = MeasurementsStatistics.Calculate(results, outlierMode);

            // return million ops/sec
            return (runCounter, finalStats.Mean);
        }

        protected abstract double Run(Stage stage, int iter, int threads, IThroughputBenchConfig config, ICache<long, int> cache);
    }

    public class ReadThroughputBenchmark : ThroughputBenchmarkBase
    {
        protected override double Run(Stage stage, int iter, int threads, IThroughputBenchConfig config, ICache<long, int> cache)
        {
            [MethodImpl(BenchmarkDotNet.Portability.CodeGenHelper.AggressiveOptimizationOption)]
            void action(int index)
            {
                long[] samples = config.GetTestData(index);
                int func(long x) => Spread(Spread(Spread(Hash32(x))));

                for (int i = 0; i < config.Iterations; i++)
                {
                    for (int s = 0; s < samples.Length; s++)
                    {
                        DeadCodeEliminationHelper.KeepAliveWithoutBoxing(cache.GetOrAdd(samples[s], func));
                    }
                }
            }

            // memory cache can queue up huge numbers of threads, wait for them to flush out
            ThreadPoolInspector.WaitForEmpty();

            // reject measurements that return too fast
            TimeSpan time = ParallelBenchmark.Run(action, threads);

            // Reject measurements that indicate memory cache eviction thread failed to run
            if (stage == Stage.Workload && time < TimeSpan.FromMilliseconds(5))
            {
                Console.WriteLine($"Warning: Execution time of {time} too fast - indicates instability.");
            }

            // Avoid dividing a very large number by a very small number.
            var millionOps = (threads * config.Samples * config.Iterations) / 1_000_000.0;
            var throughput = millionOps / time.TotalSeconds;
            if (false)
            {
#pragma warning disable CS0162 // Unreachable code detected
                Console.WriteLine($"{iter} {Format.Throughput(throughput)} ops/sec");
#pragma warning restore CS0162 // Unreachable code detected
            }

            // throughput = million ops/sec
            return throughput;
        }

        // https://lemire.me/blog/2018/08/15/fast-strongly-universal-64-bit-hashing-everywhere/
        private static readonly long a = 46601;
        private static long b = 471486146934863;
        private static long c = 7411438065634025597l;

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static int Hash32(long x)
        {
            int low = (int)x;
            int high = (int)((uint)x >> 32);
            return (int)((uint)(a * low + b * high + c) >> 32);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static int Spread(int x)
        {
            x ^= (int)((uint)x >> 17);
            x = (int)(x * 0xed5ad4bb);
            x ^= (int)((uint)x >> 11);
            x = (int)(x * 0xac4c1b51);
            x ^= (int)((uint)x >> 15);
            return x;
        }
    }

    public class UpdateThroughputBenchmark : ThroughputBenchmarkBase
    {
        protected override double Run(Stage stage, int iter, int threads, IThroughputBenchConfig config, ICache<long, int> cache)
        {
            [MethodImpl(BenchmarkDotNet.Portability.CodeGenHelper.AggressiveOptimizationOption)]
            void action(int index)
            {
                long[] samples = config.GetTestData(index);

                for (int i = 0; i < config.Iterations; i++)
                {
                    for (int s = 0; s < samples.Length; s++)
                    {
                        cache.AddOrUpdate(samples[s], (int)samples[s]);
                    }
                }
            }

            // memory cache can queue up huge numbers of threads, wait for them to flush out
            ThreadPoolInspector.WaitForEmpty();

            var time = ParallelBenchmark.Run(action, threads);

            // Reject measurements that indicate memory cache eviction thread failed to run
            if (stage == Stage.Workload && time < TimeSpan.FromMilliseconds(5))
            {
                Console.WriteLine($"Warning: Execution time of {time} too fast - indicates instability.");
            }

            var millionOps = (threads * config.Samples * config.Iterations) / 1_000_000.0;
            var throughput = millionOps / time.TotalSeconds;

            // throughput = million ops/sec
            return throughput;
        }
    }
}
