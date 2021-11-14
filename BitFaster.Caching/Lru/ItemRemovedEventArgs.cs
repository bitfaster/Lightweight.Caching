﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BitFaster.Caching.Lru
{
    /// <summary>
    /// Provides data for the ItemRemoved event.
    /// </summary>
    /// <typeparam name="K"></typeparam>
    /// <typeparam name="V"></typeparam>
    public class ItemRemovedEventArgs<K, V> : EventArgs
    {
        /// <summary>
        /// Initializes a new instance of the ItemRemovedEventArgs class using the specified key, value and reason.
        /// </summary>
        /// <param name="key">The key of the item that was removed from the cache.</param>
        /// <param name="value">The value of the item that was removed from the cache.</param>
        /// <param name="reason">The reason the item was removed from the cache.</param>
        public ItemRemovedEventArgs(K key, V value, ItemRemovedReason reason)
        {
            this.Key = key;
            this.Value = value;
            this.Reason = reason;
        }

        /// <summary>
        /// Gets the key of the item that was removed from the cache.
        /// </summary>
        public K Key { get; }

        /// <summary>
        /// Gets the value of the item that was removed from the cache.
        /// </summary>
        public V Value { get; }

        /// <summary>
        /// Gets the reason the item was removed from the cache.
        /// </summary>
        public ItemRemovedReason Reason { get; }
    }
}
