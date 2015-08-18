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

import java.util.ArrayList;
import java.util.Collection;

import org.apache.cassandra.cache.CachedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A job that gets created by an {@link IScheduler} and is then executed based on priority by the
 * {@link ScheduleManager}.
 *
 * <br>
 *
 * <br>
 *
 * The priority is calculated as:<br>
 * {@code P = (H+1) * bP}<br>
 * Where {@code P} is the priority, {@code H} is the number of hours since the job could have started and {@code bP} is
 * the base priority of the job.
 */
public abstract class ScheduledJob
{
    private static final Logger logger = LoggerFactory.getLogger(ScheduledJob.class);

    private final Collection<ScheduledTask> tasks;

    private final CachedValue<Long> cachedRunTime = new CachedValue<>();

    private JobConfiguration configuration;

    protected ScheduledJob(JobConfiguration configuration)
    {
        this(configuration, new ArrayList<ScheduledTask>());
    }

    protected ScheduledJob(JobConfiguration configuration, Collection<? extends ScheduledTask> tasks)
    {
        assert configuration != null && tasks != null;
        this.tasks = new ArrayList<>(tasks);

        this.configuration = configuration;

        if (getLastRunTime() == -1)
        {
            long lastRunTime = System.currentTimeMillis() - this.configuration.getMinimumDelay();
            for (ScheduledTask task : this.tasks)
            {
                task.setLastRunTime(lastRunTime);
            }
        }
    }

    /**
     * Set the {@link JobConfiguration} for this job.
     *
     * @param configuration
     *            The new configuration.
     */
    protected final void setConfiguration(JobConfiguration configuration)
    {
        assert configuration != null;
        this.configuration = configuration;
    }

    /**
     * Get the current {@link JobConfiguration} for this job.
     *
     * @return
     */
    protected final JobConfiguration getConfiguration()
    {
        return this.configuration;
    }

    /**
     * Update the job to get new configuration, etc.
     *
     * @return False if the job should be removed.
     */
    public abstract boolean update();

    /**
     * Execute all tasks that this scheduled job contains.
     *
     * @return True on success.
     */
    public final boolean execute()
    {
        cachedRunTime.invalidate();

        for (ScheduledTask task : tasks)
        {
            try
            {
                if (task.getLastRunTime() + configuration.getMinimumDelay() < System.currentTimeMillis())
                {
                    task.execute();
                    task.setLastRunTime(System.currentTimeMillis());
                }
            }
            catch (Exception e)
            {
                logger.warn("Unable to run task", e);
                // TODO: Handle re-running of tasks later
            }
        }

        return true;
    }

    /**
     * @return True if the job should be run only once.
     */
    public boolean shouldRunOnce()
    {
        return configuration.runOnce();
    }

    /**
     * @return True if this job is enabled.
     */
    public boolean isEnabled()
    {
        return configuration.isEnabled();
    }

    /**
     * Calculates if this job could run now.
     *
     * @return True if the job can run now.
     */
    public final boolean couldRunNow()
    {
        return (getLastRunTime() + configuration.getMinimumDelay()) < System.currentTimeMillis();
    }

    /**
     * Calculates when the job could be run next.
     *
     * @return Time in milliseconds until next run.
     */
    public long timeToNextRun()
    {
        long nextRun = (getLastRunTime() + configuration.getMinimumDelay()) - System.currentTimeMillis();

        if (nextRun < 0)
            return 0;

        return nextRun;
    }

    /**
     * Calculates the priority of the job based on the time that has passed.
     *
     * @return
     */
    public final int getPriority()
    {
        long now = System.currentTimeMillis();
        long diff = now - getLastRunTime();

        if (diff < 0)
            return -1;

        int hours = (int) (diff / (60 * 60 * 1000));

        // Using hours + 1 so that base priority works even if no time has passed
        return (hours + 1) * configuration.getBasePriority();
    }

    private long getLastRunTime()
    {
        Long cached = cachedRunTime.getValue();
        if (cached != null)
            return cached;

        long ret = -1;

        for (ScheduledTask task : tasks)
        {
            if (ret == -1 || task.getLastRunTime() < ret)
            {
                ret = task.getLastRunTime();
            }
        }

        cachedRunTime.set(ret);

        return ret;
    }

    public abstract String toString();
}
