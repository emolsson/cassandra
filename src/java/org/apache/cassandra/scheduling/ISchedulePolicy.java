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

/**
 * A scheduling policy is used by the {@link ScheduleManager} to check if a {@link ScheduledJob} should run based on the
 * custom logic implemented by the policy.
 */
public interface ISchedulePolicy
{
    /**
     * Validate if the {@link ScheduledJob} should run.
     *
     * <br>
     *
     * <br>
     *
     * A return value of {@code -1} means that the job should run according to this policy.
     *
     * <br>
     *
     * <br>
     *
     * A return value of higher than {@code -1} means that the job should be rescheduled and tried again after the given
     * time.
     *
     * @param job
     *            The job that wants to run.
     * @return -1 if the job should run, otherwise try to return a estimate in milliseconds until the job can be tried
     *         again.
     */
    public long validate(ScheduledJob job);
}
