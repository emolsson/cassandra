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

import java.util.Objects;

import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * Configuration for a {@link ScheduledJob}.
 *
 * <br>
 *
 * <br>
 *
 * The configurations are:
 * <ul>
 * <li>Minimum delay between job runs</li>
 * <li>Base priority for the job</li>
 * <li>If the job is enabled</li>
 * <li>If the job only should run once</li>
 * </ul>
 */
public class JobConfiguration
{
    public enum BasePriority
    {
        LOW(1),
        MEDIUM(2),
        HIGH(3),
        HIGHEST(100);

        private final int priority;

        private BasePriority(int prio)
        {
            this.priority = prio;
        }

        public int getPriority()
        {
            return priority;
        }
    }

    private long minimumDelay;
    private BasePriority basePriority;
    private boolean enabled;
    private boolean runOnce;

    /**
     * Create a new job configuration based on the provided parameters. Identical to calling the
     * {@link JobConfiguration#JobConfiguration(long, BasePriority, boolean, boolean) constructor} as
     *
     * <br>
     *
     * <b>new JobConfiguration(minimumDelay,basePriority,enabled,<i>false</i>)</b>
     *
     * @param minimumDelay
     *            The minimum time in seconds to wait after this job has run before it is run again.
     * @param basePriority
     *            The base priority used to calculate the "real" priority of the job.
     * @param enabled
     *            If the job should be scheduled to run.
     */
    public JobConfiguration(long minimumDelay, BasePriority basePriority, boolean enabled)
    {
        this(minimumDelay, basePriority, enabled, false);
    }

    /**
     * Create a new job configuration based on the provided parameters.
     *
     * @param minimumDelay
     *            The minimum time in seconds to wait after this job has run before it is run again.
     * @param basePriority
     *            The base priority used to calculate the "real" priority of the job.
     * @param enabled
     *            If the job should be scheduled to run.
     * @param runOnce
     *            False if the job should be rescheduled after it has run.
     */
    public JobConfiguration(long minimumDelay, BasePriority basePriority, boolean enabled, boolean runOnce)
    {
        this.minimumDelay = minimumDelay;
        this.basePriority = basePriority;
        this.enabled = enabled;
        this.runOnce = runOnce;
    }

    /**
     * Get the minimum delay in milliseconds.
     *
     * @return
     */
    public long getMinimumDelay()
    {
        return minimumDelay * 1000;
    }

    /**
     * Get the base priority for the job.
     *
     * @return
     */
    public int getBasePriority()
    {
        return basePriority.getPriority();
    }

    /**
     * @return True if the job is enabled.
     */
    public boolean isEnabled()
    {
        return enabled;
    }

    /**
     * @return True if the job should be removed after the run.
     */
    public boolean runOnce()
    {
        return runOnce;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof JobConfiguration))
            return false;

        JobConfiguration other = (JobConfiguration) o;

        return Objects.equals(minimumDelay, other.minimumDelay)
            && Objects.equals(basePriority, other.basePriority)
            && Objects.equals(enabled, other.enabled)
            && Objects.equals(runOnce, other.runOnce);
    }

    @Override
    public int hashCode()
    {
        return new HashCodeBuilder(5, 41)
                .append(minimumDelay)
                .append(basePriority)
                .append(enabled)
                .append(runOnce)
                .toHashCode();
    }
}
