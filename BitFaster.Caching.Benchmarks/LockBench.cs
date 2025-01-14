
using System.Threading;
using Benchly;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;

namespace BitFaster.Caching.Benchmarks
{
    [SimpleJob(RuntimeMoniker.Net90)]
    [MemoryDiagnoser(displayGenColumns: false)]
    [HideColumns("Job", "Median", "RatioSD", "Alloc Ratio")]
    [ColumnChart(Title ="Try enter ({JOB})")]
    public class LockBench
    {
        private int _value;
        private readonly object monitorLock = new object();
#if NET9_0_OR_GREATER
        private readonly Lock threadingLock = new Lock();
#endif

        [Benchmark(Baseline = true)]
        public void UseMonitor()
        {
            bool lockTaken = false;
            Monitor.TryEnter(monitorLock, ref lockTaken);

            if (lockTaken)
            {
                try
                {
                    _value++;
                }
                finally
                {
                    if (lockTaken)
                    {
                         Monitor.Exit(monitorLock);
                    }
                }
            }
        }

        [Benchmark()]
        public void UseLock()
        {
#if NET9_0_OR_GREATER
            if (threadingLock.TryEnter())
            {
                try
                {
                    _value++;
                }
                finally
                {
                    threadingLock.Exit();
                }
            }
#endif
        }
    }
}
