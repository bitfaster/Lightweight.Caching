﻿using System;
using System.Runtime.InteropServices;
using BitFaster.Caching.Lfu;
using FluentAssertions;
using Xunit;

namespace BitFaster.Caching.UnitTests.Lfu
{
    // This could use foreground scheduler to make it more deterministic.
    public class ConcurrentTLfuTests
    {
        private readonly TimeSpan timeToLive = TimeSpan.FromMilliseconds(200);
        private readonly int capacity = 9;
        private ConcurrentTLfu<int, string> lfu;

        private Lru.ValueFactory valueFactory = new Lru.ValueFactory();

        // on MacOS time measurement seems to be less stable, give longer pause
        private int ttlWaitMlutiplier = RuntimeInformation.IsOSPlatform(OSPlatform.OSX) ? 8 : 2;

        public ConcurrentTLfuTests()
        {
            lfu = new ConcurrentTLfu<int, string>(capacity, new ExpireAfterWrite<int, string>(timeToLive));
        }

        [Fact]
        public void WhenCalculatorIsAfterWritePolicyIsAfterWrite()
        { 
            lfu.Policy.ExpireAfterWrite.HasValue.Should().BeTrue();
            lfu.Policy.ExpireAfterWrite.Value.TimeToLive.Should().Be(timeToLive);
        }

        [Fact]
        public void WhenCalculatorIsAfterAccessPolicyIsAfterAccess()
        {
            lfu = new ConcurrentTLfu<int, string>(capacity, new ExpireAfterAccess<int, string>(timeToLive));

            lfu.Policy.ExpireAfterAccess.HasValue.Should().BeTrue();
            lfu.Policy.ExpireAfterAccess.Value.TimeToLive.Should().Be(timeToLive);
        }

        [Fact]
        public void WhenCalculatorIsCustomPolicyIsAfter()
        {
            lfu = new ConcurrentTLfu<int, string>(capacity, new TestExpiryCalculator<int, string>());

            lfu.Policy.ExpireAfter.HasValue.Should().BeTrue();
        }

        // policy can expire after write

        [Fact]
        public void WhenItemIsNotExpiredItIsNotRemoved()
        {
            lfu.GetOrAdd(1, valueFactory.Create);

            lfu.TryGet(1, out var value).Should().BeTrue();
        }

        [Fact]
        public void WhenItemIsExpiredItIsRemoved()
        {
            Timed.Execute(
                lfu,
                lfu =>
                {
                    lfu.GetOrAdd(1, valueFactory.Create);
                    return lfu;
                },
                timeToLive.MultiplyBy(ttlWaitMlutiplier),
                lfu =>
                {
                    lfu.TryGet(1, out var value).Should().BeFalse();
                }
            );
        }

        [Fact]
        public void WhenItemIsExpiredItIsRemoved2()
        {
            lfu.GetOrAdd(-1, valueFactory.Create);
            lfu.DoMaintenance();
            _ = lfu.Count;
            lfu.Clear();

            Timed.Execute(
                lfu,
                lfu =>
                {
                    lfu.GetOrAdd(1, valueFactory.Create);
                    return lfu;
                },
                TimeSpan.FromSeconds(2),
                lfu =>
                {
                    // This is a bit flaky - seems like it doesnt always
                    // remove the item
                    lfu.Policy.ExpireAfterWrite.Value.TrimExpired();
                    lfu.Count.Should().Be(0);
                }
            );
        }

        [Fact]
        public void WhenItemIsUpdatedTtlIsExtended()
        {
            Timed.Execute(
                lfu,
                lfu =>
                {
                    lfu.GetOrAdd(1, valueFactory.Create);
                    return lfu;
                },
                timeToLive.MultiplyBy(ttlWaitMlutiplier),
                lfu =>
                {
                    lfu.TryUpdate(1, "3");

                    // If we defer computing time to the maintenance loop, we
                    // need to call maintenance here for the timestamp to be updated
                    lfu.DoMaintenance();
                    lfu.TryGet(1, out var value).Should().BeTrue();
                }
            );
        }

        // TrimExpired - this just calls maintenance
        // Trim - hard to match LRU impl
        // - for non-time-based, it runs maintenance, then trims n items
        // - has no way to know how many expired items have been removed
    }
}
