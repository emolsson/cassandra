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
