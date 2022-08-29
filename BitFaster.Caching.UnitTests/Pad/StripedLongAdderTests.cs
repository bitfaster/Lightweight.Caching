﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BitFaster.Caching.Pad;
using FluentAssertions;
using Xunit;

namespace BitFaster.Caching.UnitTests.Pad
{
    public class StripedLongAdderTests
    {
        [Fact]
        public void InitialValueIsZero()
        {
            new LongAdder().Sum().Should().Be(0);
        }

        [Fact]
        public void WhenIncrementedOneIsAdded()
        {
            var adder = new LongAdder();

            adder.Increment();

            adder.Sum().Should().Be(1);
        }

        [Fact]
        public async Task WhenAddingConcurrentlySumIsCorrect()
        {
            var adder = new LongAdder();

            await Threaded.Run(4, () => 
            {
                for (int i = 0; i < 10000; i++)
                {
                    adder.Increment();
                }
            });

            adder.Sum().Should().Be(40000);
        }
    }
}
