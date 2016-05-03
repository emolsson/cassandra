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
package org.apache.cassandra.scheduling;

import org.apache.cassandra.exceptions.LeaseException;

/**
 * A leased resource.
 * <p>
 * Example usage is found in the LeaseFactory interface.
 *
 * @see org.apache.cassandra.scheduling.LeaseFactory
 */
public interface Lease
{
    /**
     * @return The time the lease will expire in milliseconds since 1970-01-01.
     */
    long getExpiration();

    /**
     * Try to renew the resource lease for the provided duration.
     *
     * @param duration The new duration for the lease in seconds.
     * @return True if able to renew the lease.
     * @throws LeaseException Thrown in case something unexpected happened while trying to renew the lease.
     */
    boolean renew(int duration) throws LeaseException;

    /**
     * Cancel the lease.
     *
     * @return True if able to cancel the lease.
     * @throws LeaseException Thrown in case something unexpected happened while trying to cancel the lease.
     */
    boolean cancel() throws LeaseException;

    /**
     * Check if the lease is valid.
     * <p>
     * The lease is valid if the current node still holds the lease.
     *
     * @return True if the lease is valid.
     */
    boolean isValid();
}