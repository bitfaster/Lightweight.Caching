﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace BitFaster.Caching.Buffers
{
    /// <summary>
    /// Provides a multi-producer, single-consumer thread-safe ring buffer. When the buffer is full,
    /// TryAdd fails and returns false. When the buffer is empty, TryTake fails and returns false.
    /// </summary>
    /// <remarks>
    /// Based on BoundedBuffer by Ben Manes.
    /// https://github.com/ben-manes/caffeine/blob/master/caffeine/src/main/java/com/github/benmanes/caffeine/cache/BoundedBuffer.java
    /// </remarks>
    [DebuggerDisplay("Count = {Count}/{Capacity}")]
    public sealed class MpscBoundedBuffer<T> where T : class
    {
        private T[] buffer;

        private readonly int mask;
        private PaddedHeadAndTail headAndTail; // mutable struct, don't mark readonly

        /// <summary>
        /// Initializes a new instance of the MpscBoundedBuffer class with the specified bounded capacity.
        /// </summary>
        /// <param name="boundedLength">The bounded length.</param>
        /// <exception cref="ArgumentOutOfRangeException"></exception>
        public MpscBoundedBuffer(int boundedLength)
        {
            if (boundedLength < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(boundedLength));
            }

            // must be power of 2 to use & slotsMask instead of %
            boundedLength = BitOps.CeilingPowerOfTwo(boundedLength);

            buffer = new T[boundedLength];
            mask = boundedLength - 1;
        }

        /// <summary>
        /// The bounded capacity.
        /// </summary>
        public int Capacity => buffer.Length;

        /// <summary>
        /// Gets the number of items contained in the buffer.
        /// </summary>
        public int Count
        {
            get
            {
                var spinner = new SpinWait();
                while (true)
                {
                    var headNow = Volatile.Read(ref headAndTail.Head);
                    var tailNow = Volatile.Read(ref headAndTail.Tail);

                    if (headNow == Volatile.Read(ref headAndTail.Head) &&
                        tailNow == Volatile.Read(ref headAndTail.Tail))
                    {
                        return GetCount(headNow, tailNow);
                    }

                    spinner.SpinOnce();
                }
            }
        }

        private int GetCount(int head, int tail)
        {
            if (head != tail)
            {
                head &= mask;
                tail &= mask;

                return head < tail ? tail - head : buffer.Length - head + tail;
            }
            return 0;
        }

        /// <summary>
        /// Tries to add the specified item.
        /// </summary>
        /// <param name="item">The item to be added.</param>
        /// <returns>A BufferStatus value indicating whether the operation succeeded.</returns>
        /// <remarks>
        /// Thread safe.
        /// </remarks>
        public BufferStatus TryAdd(T item)
        {
//#if NETSTANDARD2_0
            var localBuffer = buffer;
//#else
//            var localBuffer = buffer.AsSpan<T>();
//#endif

            int head = Volatile.Read(ref headAndTail.Head);
            int tail = Volatile.Read(ref headAndTail.Tail);
            int size = tail - head;

            if (size >= localBuffer.Length)
            {
                return BufferStatus.Full;
            }

            if (Interlocked.CompareExchange(ref this.headAndTail.Tail, tail + 1, tail) == tail)
            {
                int index = (int)(tail & mask);
                Volatile.Write(ref localBuffer[index], item);

                return BufferStatus.Success;
            }

            return BufferStatus.Contended;
        }


        /// <summary>
        /// Tries to remove an item.
        /// </summary>
        /// <param name="item">The item to be removed.</param>
        /// <returns>A BufferStatus value indicating whether the operation succeeded.</returns>
        /// <remarks>
        /// Thread safe for single try take/drain + multiple try add.
        /// </remarks>
        public BufferStatus TryTake(out T item)
        {
            int head = Volatile.Read(ref headAndTail.Head);
            int tail = Volatile.Read(ref headAndTail.Tail);
            int size = tail - head;

            if (size == 0)
            {
                item = default;
                return BufferStatus.Empty;
            }

            int index = head & mask;

            item = Volatile.Read(ref buffer[index]);

            if (item == null)
            {
                // not published yet
                return BufferStatus.Contended;
            }

            Volatile.Write(ref buffer[index], null);
            Volatile.Write(ref this.headAndTail.Head, ++head);
            return BufferStatus.Success;
        }

        /// <summary>
        /// Drains the buffer into the specified array segment.
        /// </summary>
        /// <param name="output">The output buffer</param>
        /// <returns>The number of items written to the output buffer.</returns>
        /// <remarks>
        /// Thread safe for single try take/drain + multiple try add.
        /// </remarks>
#if NETSTANDARD2_0
        public int DrainTo(ArraySegment<T> output)
        {

            int head = Volatile.Read(ref headAndTail.Head);
            int tail = Volatile.Read(ref headAndTail.Tail);
            int size = tail - head;

            if (size == 0)
            {
                return 0;
            }

            int outCount = 0;

            do
            {
                int index = head & mask;

                T item = Volatile.Read(ref buffer[index]);

                if (item == null)
                {
                    // not published yet
                    break;
                }

                Volatile.Write(ref buffer[index], null);


                output.Array[output.Offset + outCount++] = item;

                head++;
            }
            while (head != tail && outCount < output.Count);

            Volatile.Write(ref this.headAndTail.Head, head);

            return outCount;
        }

#else
        // https://docs.microsoft.com/en-us/dotnet/api/system.runtime.compilerservices.unsafe?view=dotnet-plat-ext-2.2
        // https://mattwarren.org/2016/09/14/Subverting-.NET-Type-Safety-with-System.Runtime.CompilerServices.Unsafe/
        // https://github.com/dotnet/runtime/issues/16143
        
        // Unsafe version
        public unsafe int DrainTo(ArraySegment<T> output)
        {
            Span<T> localBuffer = buffer;
            Span<T> localOutput = output;

            int head = Volatile.Read(ref headAndTail.Head);
            int tail = Volatile.Read(ref headAndTail.Tail);
            int size = tail - head;

            if (size == 0)
            {
                return 0;
            }

            int outCount = 0;

            ref T refOutItem = ref MemoryMarshal.GetReference(localOutput);
            ref T refOutEnd = ref Unsafe.Add(ref refOutItem, localOutput.Length);

            ref T refBuffStart = ref MemoryMarshal.GetReference(localBuffer);

            do
            {
                int index = head & mask;

                ref T refBuffItem = ref Unsafe.Add(ref refBuffStart, index);

                T item = Volatile.Read(ref refBuffItem);

                if (item == null)
                {
                    // not published yet
                    break;
                }

                Volatile.Write(ref refBuffItem, null);

                refOutItem = item;
                refOutItem = ref Unsafe.Add(ref refOutItem, 1);
                outCount++;

                head++;
            }
            while (head != tail && Unsafe.IsAddressLessThan(ref refOutItem, ref refOutEnd));

            Volatile.Write(ref this.headAndTail.Head, head);

            return outCount;
        }

        // Pointer version, this is only valid for x64
        // This is known to crash, so is not correct.
        public unsafe int DrainTo1(ArraySegment<T> output)
        {
            int head = Volatile.Read(ref headAndTail.Head);
            int tail = Volatile.Read(ref headAndTail.Tail);
            int size = tail - head;

            if (size == 0)
            {
                return 0;
            }

            // pretend the array is of type long (same size as ptr), then do ptr arithmetic
            ref long buffAsRefLong = ref Unsafe.As<T, long>(ref buffer[0]); 
            ref long outAsRefLong = ref Unsafe.As<T, long>(ref output.Array[output.Offset]);

            fixed (long* buffPtr = &buffAsRefLong, outPtr = &outAsRefLong)
            {
                long* endOutPtr = outPtr + output.Count;
                long* currOutPtr = outPtr;

                do
                {
                    int index = head & mask;

                    long* itemPtr = buffPtr + index;
                    *currOutPtr = Volatile.Read(ref *itemPtr);

                    if (*currOutPtr == 0)
                    {
                        // not published yet
                        break;
                    }

                    Volatile.Write(ref *itemPtr, 0);

                    currOutPtr++;
                    head++;
                }
                while (head != tail && currOutPtr < endOutPtr);

                Volatile.Write(ref this.headAndTail.Head, head);

                return (int)(currOutPtr - outPtr);
            }
        }
#endif

        /// <summary>
        /// Removes all values from the buffer.
        /// </summary>
        /// <remarks>
        /// Not thread safe.
        /// </remarks>
        public void Clear()
        {
            buffer = new T[buffer.Length];
            headAndTail = new PaddedHeadAndTail();
        }
    }
}
