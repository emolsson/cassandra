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

import java.io.IOException;
import java.util.Objects;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
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
    public static IVersionedSerializer<JobConfiguration> serializer = new JobConfigurationSerializer();

    public static enum BasePriority
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

        public static BasePriority fromInt(int i)
        {
            for (BasePriority bp : values())
            {
                if (bp.priority == i)
                    return bp;
            }
            throw new IllegalArgumentException("Unknown JobConfiguration.BasePriority: " + i);
        }
    }

    private long minimumDelay;
    private BasePriority basePriority;
    private boolean enabled;
    private boolean runOnce;

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
    private JobConfiguration(long minimumDelay,
            BasePriority basePriority,
            boolean enabled,
            boolean runOnce)
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

    /**
     * Helper class used to create a {@link ScheduledRepairJob}.
     */
    public static class Builder
    {
        private BasePriority priority = BasePriority.LOW;
        private boolean enabled = true;
        private boolean runOnce = false;
        private Long minimumDelay = null;

        public Builder()
        {

        }

        /**
         * Set the enabled flag for the {@link JobConfiguration}.
         *
         * @param keyspace
         * @return this
         */
        public Builder withEnabled(boolean enabled)
        {
            this.enabled = enabled;
            return this;
        }

        /**
         * Set the run once flag for the {@link JobConfiguration}.
         *
         * @param keyspace
         * @return this
         */
        public Builder withRunOnce(boolean runOnce)
        {
            this.runOnce = runOnce;
            return this;
        }

        /**
         * Set the minimum delay for the {@link JobConfiguration}.
         *
         * @param keyspace
         * @return this
         */
        public Builder withMinimumDelay(long minimumDelay)
        {
            this.minimumDelay = minimumDelay;
            return this;
        }

        /**
         * Set the priority for the {@link JobConfiguration}.
         *
         * @param keyspace
         * @return this
         */
        public Builder withPriority(BasePriority priority)
        {
            this.priority = priority;
            return this;
        }

        /**
         * Build the {@link JobConfiguration}.
         *
         * @return A {@link JobConfiguration} based on the attributes specified.
         */
        public JobConfiguration build()
        {
            if (minimumDelay == null)
                throw new ConfigurationException("There must be a minimum delay defined for a job configuration.");

            return new JobConfiguration(minimumDelay, priority, enabled, runOnce);
        }
    }

    private static class JobConfigurationSerializer implements IVersionedSerializer<JobConfiguration>
    {
        @Override
        public void serialize(JobConfiguration t, DataOutputPlus out, int version) throws IOException
        {
            out.writeLong(t.minimumDelay);
            out.writeInt(t.basePriority.priority);
            out.writeBoolean(t.enabled);
            out.writeBoolean(t.runOnce);
        }

        @Override
        public JobConfiguration deserialize(DataInputPlus in, int version) throws IOException
        {
            long minimumDelay = in.readLong();
            BasePriority basePriority = BasePriority.fromInt(in.readInt());
            boolean enabled = in.readBoolean();
            boolean runOnce = in.readBoolean();

            return new JobConfiguration(minimumDelay, basePriority, enabled, runOnce);
        }

        @Override
        public long serializedSize(JobConfiguration t, int version)
        {
            long size = TypeSizes.sizeof(t.minimumDelay);
            size += TypeSizes.sizeof(t.basePriority.priority);
            size += TypeSizes.sizeof(t.enabled);
            size += TypeSizes.sizeof(t.runOnce);

            return size;
        }

    }
}
