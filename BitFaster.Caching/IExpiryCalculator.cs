﻿using System;

namespace BitFaster.Caching
{
    /// <summary>
    /// Defines a mechanism to calculate when cache entries expire.
    /// </summary>
    public interface IExpiryCalculator<K, V>
    {
        /// <summary>
        /// Specify the inital time to expire after an entry is created.
        /// </summary>
        TimeSpan GetExpireAfterCreate(K key, V value);

        /// <summary>
        /// Specify the time to expire after an entry is read. The current TTL may be
        /// be returned to not modify the expiration time.
        /// </summary>
        TimeSpan GetExpireAfterRead(K key, V value, TimeSpan currentTtl);

        /// <summary>
        /// Specify the time to expire after an entry is updated.The current TTL may be
        /// be returned to not modify the expiration time.
        /// </summary>
        TimeSpan GetExpireAfterUpdate(K key, V value, TimeSpan currentTtl);
    }
}
