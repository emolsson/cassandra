/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.cache;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A thread safe cached value that can be invalidated when needed.
 */
public class CachedValue<T>
{
    private ReadWriteLock lock = new ReentrantReadWriteLock();

    private T cachedValue;

    /**
     * Create an empty new cached value.
     */
    public CachedValue()
    {
        this(null);
    }

    /**
     * Create a new cached value with an initial value set.
     *
     * @param value
     */
    public CachedValue(T value)
    {
        cachedValue = value;
    }

    /**
     * Set the cached value to the provided value.
     *
     * @param value
     * @return
     */
    public CachedValue<T> set(T value)
    {
        lock.writeLock().lock();
        try
        {
            cachedValue = value;

            return this;
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    /**
     * Invalidate the current value.
     *
     * @return
     */
    public CachedValue<T> invalidate()
    {
        lock.writeLock().lock();
        try
        {
            cachedValue = null;

            return this;
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    /**
     * Get the cached value.
     *
     * @return The cached value or null if invalidated.
     */
    public T getValue()
    {
        lock.readLock().lock();
        try
        {
            return cachedValue;
        }
        finally
        {
            lock.readLock().unlock();
        }
    }
}