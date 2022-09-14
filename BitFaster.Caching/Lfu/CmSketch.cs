﻿using System;
using System.Collections.Generic;

#if !NETSTANDARD2_0
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
#endif

namespace BitFaster.Caching.Lfu
{
    /// <summary>
    /// A probabilistic data structure used to estimate the frequency of a given value. Periodic aging reduces the
    /// accumulated count across all values over time, such that a historic popular value will decay to zero frequency
    /// over time if it is not accessed.
    /// </summary>
    /// This is a direct C# translation of FrequencySketch in the Caffeine library by ben.manes@gmail.com (Ben Manes).
    /// https://github.com/ben-manes/caffeine
    public sealed class CmSketch<T, TAVX> where TAVX : struct, IAvx2Toggle
    {
        // A mixture of seeds from FNV-1a, CityHash, and Murmur3
        private static readonly ulong[] Seed = { 0xc3a5c85c97cb3127L, 0xb492b66fbe98f273L, 0x9ae16a3b2f90404fL, 0xcbf29ce484222325L};
        private static readonly long ResetMask = 0x7777777777777777L;
        private static readonly long OneMask = 0x1111111111111111L;

        private int sampleSize;
        private int tableMask;
        private long[] table;
        private int size;

        private readonly IEqualityComparer<T> comparer;

        /// <summary>
        /// Initializes a new instance of the CmSketch class with the specified maximum size and equality comparer.
        /// </summary>
        /// <param name="maximumSize">The maximum size.</param>
        /// <param name="comparer">The equality comparer.</param>
        public CmSketch(long maximumSize, IEqualityComparer<T> comparer)
        {
            EnsureCapacity(maximumSize);
            this.comparer = comparer;
        }

        /// <summary>
        /// Gets the reset sample size.
        /// </summary>
        public int ResetSampleSize => this.sampleSize;

        /// <summary>
        /// Gets the size.
        /// </summary>
        public int Size => this.size;

        /// <summary>
        /// Estimate the frequency of the specified value.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>The estimated frequency of the value.</returns>
        public int EstimateFrequency(T value)
        {
#if NETSTANDARD2_0
            return EstimateFrequencyStd(value);
#else
            
            TAVX avx2 = default;

            if (avx2.IsSupported)
            {
                return EstimateFrequencyAvx(value);
            }
            else
            {
                return EstimateFrequencyStd(value);
            }
#endif
        }

        /// <summary>
        /// Increment the count of the specified value.
        /// </summary>
        /// <param name="value">The value.</param>
        public void Increment(T value)
        {
#if NETSTANDARD2_0
            IncrementStd(value);
#else

            TAVX avx2 = default;

            if (avx2.IsSupported)
            {
                IncrementAvx(value);
            }
            else
            {
                IncrementStd(value);
            }
#endif
        }

        /// <summary>
        /// Clears the count for all items.
        /// </summary>
        public void Clear()
        {
            table = new long[table.Length];
            size = 0;
        }

        private int EstimateFrequencyStd(T value)
        {
            int hash = Spread(value.GetHashCode());

            int start = (hash & 3) << 2;
            int frequency = int.MaxValue;

            for (int i = 0; i < 4; i++)
            {
                int index = IndexOf(hash, i);
                int count = (int)(((ulong)table[index] >> ((start + i) << 2)) & 0xfL);
                frequency = Math.Min(frequency, count);
            }
            return frequency;
        }

        private void IncrementStd(T value)
        {
            int hash = Spread(value.GetHashCode());
            int start = (hash & 3) << 2;

            // Loop unrolling improves throughput by 5m ops/s
            int index0 = IndexOf(hash, 0);
            int index1 = IndexOf(hash, 1);
            int index2 = IndexOf(hash, 2);
            int index3 = IndexOf(hash, 3);

            bool added = IncrementAt(index0, start);
            added |= IncrementAt(index1, start + 1);
            added |= IncrementAt(index2, start + 2);
            added |= IncrementAt(index3, start + 3);

            if (added && (++size == sampleSize))
            {
                Reset();
            }
        }

        private bool IncrementAt(int i, int j)
        {
            int offset = j << 2;
            long mask = (0xfL << offset);
            if ((table[i] & mask) != mask)
            {
                table[i] += (1L << offset);
                return true;
            }
            return false;
        }

        private void Reset()
        {
            int count = 0;
            for (int i = 0; i < table.Length; i++)
            {
                count += BitOps.BitCount(table[i] & OneMask);
                table[i] = (long)((ulong)table[i] >> 1) & ResetMask;
            }
            size = (size - (count >> 2)) >> 1;
        }

        private void EnsureCapacity(long maximumSize)
        {
            int maximum = (int)Math.Min(maximumSize, int.MaxValue >> 1);

            table = new long[(maximum == 0) ? 1 : BitOps.CeilingPowerOfTwo(maximum)];
            tableMask = Math.Max(0, table.Length - 1);
            sampleSize = (maximumSize == 0) ? 10 : (10 * maximum);

            size = 0;
        }

        private int IndexOf(int item, int i)
        {
            ulong hash = ((ulong)item + Seed[i]) * Seed[i];
            hash += (hash >> 32);
            return ((int)hash) & tableMask;
        }

        private int Spread(int x)
        {
            uint y = (uint)x;
            y = ((y >> 16) ^ y) * 0x45d9f3b;
            y = ((y >> 16) ^ y) * 0x45d9f3b;
            return (int)((y >> 16) ^ y);
        }

#if !NETSTANDARD2_0
        private unsafe int EstimateFrequencyAvx(T value)
        {
            int hash = Spread(value.GetHashCode());
            int start = (hash & 3) << 2;

            fixed (long* tablePtr = &table[0])
            {
                var tableVector = Avx2.GatherVector256(tablePtr, IndexesOfAvx(hash), 8).AsUInt64();

                Vector256<ulong> starts = Vector256.Create(0UL, 1UL, 2UL, 3UL);
                starts = Avx2.Add(starts, Vector256.Create((ulong)start));
                starts = Avx2.ShiftLeftLogical(starts, 2);

                tableVector = Avx2.ShiftRightLogicalVariable(tableVector, starts);
                tableVector = Avx2.And(tableVector, Vector256.Create(0xfUL));

                // Note: this is faster than skipping then doing the Min checks on long values
                Vector256<int> permuteMask = Vector256.Create(0, 2, 4, 6, 1, 3, 5, 7);
                Vector128<int> f = Avx2.PermuteVar8x32(tableVector.AsInt32(), permuteMask)
                    .GetLower();

                return Math.Min(
                    f.GetElement(0),
                        Math.Min(f.GetElement(1),
                            Math.Min(f.GetElement(2), f.GetElement(3)))
                    );
            }
        }

        private unsafe void IncrementAvx(T value)
        {
            int hash = Spread(value.GetHashCode());
            int start = (hash & 3) << 2;

            Vector128<int> indexes = IndexesOfAvx(hash);

            fixed (long* tablePtr = &table[0])
            {
                var tableVector = Avx2.GatherVector256(tablePtr, indexes, 8);

                // offset = j << 2, where j [start+0, start+1, start+2, start+3]
                Vector256<ulong> offset = Vector256.Create((ulong)start);
                Vector256<ulong> add = Vector256.Create(0UL, 1UL, 2UL, 3UL);
                offset = Avx2.Add(offset, add);
                offset = Avx2.ShiftLeftLogical(offset, 2);

                // mask = (0xfL << offset)
                Vector256<long> fifteen = Vector256.Create(0xfL);
                Vector256<long> mask = Avx2.ShiftLeftLogicalVariable(fifteen, offset);

                // (table[i] & mask) != mask)
                // Note masked is 'equal' - therefore use AndNot below
                Vector256<long> masked = Avx2.CompareEqual(Avx2.And(tableVector, mask), mask);

                // 1L << offset
                Vector256<long> inc = Avx2.ShiftLeftLogicalVariable(Vector256.Create(1L), offset);

                // Mask to zero out non matches (add zero below) - first operand is NOT then AND result (order matters)
                inc = Avx2.AndNot(masked, inc);

                *(tablePtr + indexes.GetElement(0)) += inc.GetElement(0);
                *(tablePtr + indexes.GetElement(1)) += inc.GetElement(1);
                *(tablePtr + indexes.GetElement(2)) += inc.GetElement(2);
                *(tablePtr + indexes.GetElement(3)) += inc.GetElement(3);

                bool wasInc = Avx2.MoveMask(masked.AsByte()) != 0; // _mm256_movemask_epi8

                if (wasInc && (++size == sampleSize))
                {
                    Reset();
                }
            }
        }

        private Vector128<int> IndexesOfAvx(int item)
        {
            Vector256<ulong> VectorSeed = Vector256.Create(0xc3a5c85c97cb3127L, 0xb492b66fbe98f273L, 0x9ae16a3b2f90404fL, 0xcbf29ce484222325L);
            Vector256<ulong> hash = Vector256.Create((ulong)item);
            hash = Avx2.Add(hash, VectorSeed);

            // unfortunately no vector multiply until .NET 7
            hash = Vector256.Create(
                hash.GetElement(0) * 0xc3a5c85c97cb3127L,
                hash.GetElement(1) * 0xb492b66fbe98f273L,
                hash.GetElement(2) * 0x9ae16a3b2f90404fL,
                hash.GetElement(3) * 0xcbf29ce484222325L);

            Vector256<ulong> shift = Vector256.Create(32UL);
            Vector256<ulong> shifted = Avx2.ShiftRightLogicalVariable(hash, shift);
            hash = Avx2.Add(hash, shifted);

            // Move            [a1, a2, b1, b2, c1, c2, d1, d2]
            // To              [a1, b1, c1, d1, a2, b2, c2, d2]
            // then GetLower() [a1, b1, c1, d1]
            Vector256<int> permuteMask = Vector256.Create(0, 2, 4, 6, 1, 3, 5, 7);
            Vector128<int> f = Avx2.PermuteVar8x32(hash.AsInt32(), permuteMask)
                .GetLower();

            Vector128<int> maskVector = Vector128.Create(tableMask);
            return Avx2.And(f, maskVector);
        }
#endif
    }
}
