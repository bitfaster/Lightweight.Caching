﻿using System;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;

namespace BitFaster.Caching.Lru
{
    /// <summary>
    /// Time aware Least Recently Used (TLRU) is a variant of LRU which discards the least 
    /// recently used items first, and any item that has expired.
    /// </summary>
    /// <remarks>
    /// This class measures time using Stopwatch.GetTimestamp() with a resolution of ~1us.
    /// </remarks>
    [DebuggerDisplay("TTL = {TimeToLive,nq})")]
    public readonly struct TlruStopwatchPolicy<K, V> : IItemPolicy<K, V, LongTickCountLruItem<K, V>>
    {
        // On some platforms (e.g. MacOS), stopwatch and timespan have different resolution
        private static readonly double stopwatchAdjustmentFactor = Stopwatch.Frequency / (double)TimeSpan.TicksPerSecond;
        private readonly long timeToLive;

        /// <summary>
        /// Initializes a new instance of the TLruLongTicksPolicy class with the specified time to live.
        /// </summary>
        /// <param name="timeToLive">The time to live.</param>
        public TlruStopwatchPolicy(TimeSpan timeToLive)
        {
            this.timeToLive = ToTicks(timeToLive);
        }

        ///<inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public LongTickCountLruItem<K, V> CreateItem(K key, V value)
        {
            return new LongTickCountLruItem<K, V>(key, value, Stopwatch.GetTimestamp());
        }

        ///<inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Touch(LongTickCountLruItem<K, V> item)
        {
            item.WasAccessed = true;
        }

        ///<inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Update(LongTickCountLruItem<K, V> item)
        {
            item.TickCount = Stopwatch.GetTimestamp();
        }

        ///<inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ShouldDiscard(LongTickCountLruItem<K, V> item)
        {
            if (Stopwatch.GetTimestamp() - item.TickCount > this.timeToLive)
            {
                return true;
            }

            return false;
        }

        ///<inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool CanDiscard()
        {
            return true;
        }

        ///<inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ItemDestination RouteHot(LongTickCountLruItem<K, V> item)
        {
            if (this.ShouldDiscard(item))
            {
                return ItemDestination.Remove;
            }

            if (item.WasAccessed)
            {
                return ItemDestination.Warm;
            }

            return ItemDestination.Cold;
        }

        ///<inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ItemDestination RouteWarm(LongTickCountLruItem<K, V> item)
        {
            if (this.ShouldDiscard(item))
            {
                return ItemDestination.Remove;
            }

            if (item.WasAccessed)
            {
                return ItemDestination.Warm;
            }

            return ItemDestination.Cold;
        }

        ///<inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ItemDestination RouteCold(LongTickCountLruItem<K, V> item)
        {
            if (this.ShouldDiscard(item))
            {
                return ItemDestination.Remove;
            }

            if (item.WasAccessed)
            {
                return ItemDestination.Warm;
            }

            return ItemDestination.Remove;
        }

        ///<inheritdoc/>
        public TimeSpan TimeToLive => FromTicks(timeToLive);

        /// <summary>
        /// Convert from TimeSpan to ticks.
        /// </summary>
        /// <param name="timespan">The time represented as a TimeSpan.</param>
        /// <returns>The time represented as ticks.</returns>
        public static long ToTicks(TimeSpan timespan)
        {
            // mac adjustment factor is 100, giving lowest maximum TTL on mac platform - use same upper limit on all platforms for consistency
            // this also avoids overflow when multipling long.MaxValue by 1.0
            double maxTicks = long.MaxValue / 100.0d;
            double value = timespan.Ticks * stopwatchAdjustmentFactor;

            if (timespan <= TimeSpan.Zero || value >= maxTicks)
            {
                TimeSpan maxRepresentable = TimeSpan.FromTicks((long)maxTicks);
                Ex.ThrowArgOutOfRange(nameof(timespan), $"Value must be greater than zero and less than {maxRepresentable}");
            }

            return (long)(value);
        }

        /// <summary>
        /// Convert from ticks to a TimeSpan.
        /// </summary>
        /// <param name="ticks">The time represented as ticks.</param>
        /// <returns>The time represented as a TimeSpan.</returns>
        public static TimeSpan FromTicks(long ticks)
        {
            double value = ticks / stopwatchAdjustmentFactor;
            return TimeSpan.FromTicks((long)value);
        }
    }
}
