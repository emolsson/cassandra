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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScheduleManager
{
    private static final Logger logger = LoggerFactory.getLogger(ScheduleManager.class);

    public static final String SCHEDULE_LOCK = "schedule_lock";

    private static final long updateDelay = 3600;

    public static final ScheduleManager instance = new ScheduleManager();

    private final ScheduledJobQueue scheduledJobs = new ScheduledJobQueue();

    private final Object updateLock = new Object();

    private volatile ScheduledFuture<?> updateFuture;

    private volatile ScheduledJobRunner jobRunner;

    private Collection<IScheduler> schedulers;
    private Collection<ISchedulePolicy> schedulePolicies;

    private ScheduleManager()
    {
    }

    public void startup()
    {
        schedulers = Collections.unmodifiableCollection(DatabaseDescriptor.getMaintenanceSchedulers());
        schedulePolicies = Collections.unmodifiableCollection(DatabaseDescriptor.getMaintenanceSchedulePolicies());

        if (schedulers.isEmpty())
        {
            logger.info("No schedulers specified");
            return;
        }

        jobRunner = new ScheduledJobRunner(scheduledJobs, schedulePolicies);

        updateFuture = ScheduledExecutors.scheduledLongTasks.scheduleWithFixedDelay(new JobUpdateTask(),
                DatabaseDescriptor.getMaintenaneSchedulerStartupDelay(), updateDelay, TimeUnit.SECONDS);
    }

    @VisibleForTesting
    public void startup(ISchedulePolicy policy)
    {
        schedulePolicies = Arrays.asList(policy);

        jobRunner = new ScheduledJobRunner(scheduledJobs, schedulePolicies);
    }

    @VisibleForTesting
    public void shutdown()
    {
        if (updateFuture != null)
        {
            updateFuture.cancel(true);
        }
        if (jobRunner != null)
        {
            jobRunner.stop();
        }
    }

    /**
     * Schedule a job to run.
     *
     * @param job
     */
    public void schedule(ScheduledJob job)
    {
        scheduledJobs.add(job, ScheduleManager::compareJob);

        jobRunner.maybeReschedule();
    }

    /**
     * Force an update of all jobs.
     *
     * Might reschedule the job runner.
     */
    public void forceUpdate()
    {
        updateJobs();

        jobRunner.maybeReschedule();
    }

    /**
     * Scheduled task that updates jobs and might reschedule the job runner.
     */
    private class JobUpdateTask implements Runnable
    {
        @Override
        public void run()
        {
            updateJobs();

            jobRunner.maybeReschedule();
        }
    }

    private void updateJobs()
    {
        if (schedulers == null)
            return;

        synchronized (updateLock)
        {
            scheduledJobs.updateJobs();

            for (IScheduler scheduler : schedulers)
            {
                scheduledJobs.addAll(scheduler.createNewJobs(), ScheduleManager::compareJob);
            }
        }
    }

    @VisibleForTesting
    public static ScheduleManager getManagerForTest()
    {
        return new ScheduleManager();
    }

    public static int compareJob(ScheduledJob j1, ScheduledJob j2)
    {
        if (!j1.isEnabled())
            return 1;
        if (!j2.isEnabled())
            return -1;

        if (!j1.couldRunNow())
            return 1;
        if (!j2.couldRunNow())
            return -1;

        int o1Prio = j1.getPriority();
        int o2Prio = j2.getPriority();

        if (o2Prio > o1Prio)
            return 1;
        else if (o1Prio > o2Prio)
            return -1;
        else
            return j1.toString().compareTo(j2.toString());
    }
}
