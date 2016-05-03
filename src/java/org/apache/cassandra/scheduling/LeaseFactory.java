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

import java.util.Map;
import java.util.Optional;

import org.apache.cassandra.exceptions.LeaseException;

/**
 * Interface for a lease factory.
 * <p>
 * The lifecycle of a lease begins with requesting a new lease by calling {@link #newLease(String, int, Map)}.
 * If the lease was acquired it's then possible to manipulate the lease by the methods defined in the interface {@link Lease}.
 * The caller is responsible to call {@link Lease#renew(int)} to renew the lease for the needed duration.
 * When done with the leased resource the caller uses the {@link Lease#cancel()} method to release the leased resource.
 * <p>
 * An example usage:
 * <pre>
 * Lease lease = LeaseFactoryImplementation.newLease("a-resource", 1, new HashMap&lt;&gt;());
 * if (lease != null)
 * {
 *     try {
 *         // Do something with the resource.
 *         if (lease.isValid()) { // Check if the lease is still valid.
 *             lease.renew(60); // We need the lease for another 60 seconds from now.
 *             // Do some more things with the leased resource.
 *         }
 *     } finally {
 *         lease.cancel();
 *     }
 * }
 * </pre>
 */
public interface LeaseFactory
{

    /**
     * Try to get a lease for the specified resource.
     * <p>
     * The priority is compared to other nodes priority for this resource to avoid starvation.
     *
     * @param resource The resource to lease
     * @param priority The priority this lease has
     * @param metadata The metadata associated with the lease
     * @return The {@link Lease} if acquired, otherwise empty.
     * @throws LeaseException Thrown if unable to lease the resource
     */
    Optional<Lease> newLease(String resource, int priority, Map<String, String> metadata) throws LeaseException;
}
