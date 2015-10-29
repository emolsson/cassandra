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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.scheduling.DistributedLock.Lock;
import org.apache.cassandra.scheduling.DistributedLock.LockException;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScheduleManager
{
    private static final Logger logger = LoggerFactory.getLogger(ScheduleManager.class);

    public static final String SCHEDULE_LOCK = "schedule_lock";

    private static final long updateDelay = 3600;

    public static final ScheduleManager instance = new ScheduleManager();

    private final ScheduledJobQueue scheduledJobs = new ScheduledJobQueue();

    private final JobRunTask runTask = new JobRunTask();

    private final Object updateLock = new Object();

    private volatile ScheduledFuture<?> runFuture;
    private volatile ScheduledFuture<?> updateFuture;

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

        updateFuture = ScheduledExecutors.scheduledLongTasks.scheduleWithFixedDelay(new JobUpdateTask(),
                DatabaseDescriptor.getMaintenaneSchedulerStartupDelay(), updateDelay, TimeUnit.SECONDS);
    }

    @VisibleForTesting
    public void startup(ISchedulePolicy policy)
    {
        schedulePolicies = Arrays.asList(policy);
    }

    @VisibleForTesting
    public void shutdown()
    {
        if (updateFuture != null)
        {
            updateFuture.cancel(true);
        }
        if (runFuture != null)
        {
            runFuture.cancel(true);
        }
    }

    /**
     * Schedule a job to run.
     *
     * @param job
     */
    public void schedule(ScheduledJob job)
    {
        scheduledJobs.add(job);

        runTask.maybeReschedule();
    }

    /**
     * Force an update of all jobs.
     *
     * Might reschedule the job runner.
     */
    public void forceUpdate()
    {
        updateJobs();

        runTask.maybeReschedule();
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

            runTask.maybeReschedule();
        }
    }

    /**
     * A task that tries to run scheduled jobs and checks with the policies if the job should have permission to run
     * before running it.
     *
     * Reschedules itself to run when the next job should run.
     *
     * Uses distributed locks to synchronize between the nodes.
     */
    private class JobRunTask implements Runnable
    {
        private final AtomicBoolean running = new AtomicBoolean(false);

        private final Object schedulingLock = new Object();

        private volatile long nextScheduledRunTime = -1;

        @Override
        public void run()
        {
            if (!running.compareAndSet(false, true))
            {
                logger.debug("Scheduled job shouldn't be running, but was.");
                return;
            }

            long delay = -1;

            try
            {
                ScheduledJob job = scheduledJobs.peek();

                if (job != null && job.couldRunNow())
                {
                    delay = validate(job);

                    if (delay == -1)
                        tryRunJob(job);
                }
            }
            finally
            {
                if (!running.compareAndSet(true, false))
                    logger.warn("Scheduled job should be running, but wasn't.");

                synchronized (schedulingLock)
                {
                    nextScheduledRunTime = -1;

                    maybeReschedule(delay);
                }
            }
        }

        private long validate(ScheduledJob job)
        {
            long delay = -1;

            if (schedulePolicies == null)
                return delay;

            for (ISchedulePolicy policy : schedulePolicies)
            {
                long tmp = policy.validate(job);

                if (tmp > delay)
                    delay = tmp;
            }

            return delay;
        }

        private void tryRunJob(ScheduledJob job)
        {
            try (Lock lock = job.getLock())
            {
                runJob(job);
            }
            catch (LockException | IOException e)
            {
                if (e.getCause() != null)
                    logger.warn("Unable to get schedule lock", e);
            }
        }

        private void runJob(ScheduledJob job)
        {
            try
            {
                job.execute();

                if (job.shouldRunOnce())
                    scheduledJobs.remove(job);

                // TODO: Save job status
            }
            catch (Exception e)
            {
                logger.warn("Unable to run job", e);
                // TODO: Fallback handling
            }
        }

        /**
         * Try to reschedule the task runner if it isn't already running.
         */
        public void maybeReschedule()
        {
            synchronized (schedulingLock)
            {
                maybeReschedule(-1);
            }
        }

        private void maybeReschedule(long wantedDelay)
        {
            long delay = wantedDelay == -1 ? delayToNextJob() : wantedDelay;

            if (delay >= 0)
            {
                if (running.get())
                    return;

                delay += 1000;

                long now = System.currentTimeMillis();

                long oldScheduledRunTime = nextScheduledRunTime;
                nextScheduledRunTime = now + delay;

                if (oldScheduledRunTime != -1)
                {
                    // Avoid rescheduling if within one minute
                    if (difference(oldScheduledRunTime, nextScheduledRunTime) < 60000)
                    {
                        return;
                    }

                    if (delay < DistributedLock.getLockTime())
                        delay = DistributedLock.getLockTime();
                }

                logger.info("Next scheduled job at '{}'", new DateTime(now + delay));

                ScheduledFuture<?> oldFuture = runFuture;
                runFuture = ScheduledExecutors.scheduledLongTasks.schedule(runTask, delay,
                        TimeUnit.MILLISECONDS);

                if (oldFuture != null && !oldFuture.isDone())
                    oldFuture.cancel(false);
            }
        }

        private long difference(long l1, long l2)
        {
            if (l1 < l2)
                return l2 - l1;
            else
                return l1 - l2;
        }

        private long delayToNextJob()
        {
            ScheduledJob nextJob = scheduledJobs.peek();

            if (nextJob != null && nextJob.isEnabled())
                return nextJob.timeToNextRun();

            return -1;
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
                scheduledJobs.addAll(scheduler.createNewJobs());
            }
        }
    }

    @VisibleForTesting
    public static ScheduleManager getManagerForTest()
    {
        return new ScheduleManager();
    }
}
