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
/**
 * The scheduling subsystem is handling the scheduling of distributed tasks that needs to be coordinated between nodes.
 *
 * {@link org.apache.cassandra.scheduling.LeaseFactory} is the interface for lease factories used to coordinate access to resources(e.g. a distributed task).
 *
 * {@link org.apache.cassandra.scheduling.Lease} is the interface for operations that can be performed on resource leases.
 *
 * {@link org.apache.cassandra.scheduling.CasLeaseFactory} is the implementation of a distributed lease factory using CAS.
 */
package org.apache.cassandra.scheduling;