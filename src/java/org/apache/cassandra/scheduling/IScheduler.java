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

import java.util.Collection;

/**
 * Classes that implements this interface can create maintenance jobs and tasks that gets scheduled by the
 * {@link ScheduleManager}.
 *
 * <br>
 *
 * <br>
 *
 * For a scheduler to run it must be specified in <b>~/conf/cassandra.yaml</b> in <i>'maintenance_schedulers'</i>.
 */
public interface IScheduler
{
    /**
     * Generates a collection of the new jobs that should run.
     *
     * @return
     */
    public Collection<ScheduledJob> createNewJobs();
}
