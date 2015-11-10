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
import java.util.Collection;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.scheduling.DistributedLock.Lock;
import org.apache.cassandra.scheduling.DistributedLock.LockException;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A runnable that tries to run scheduled jobs and checks with the policies if the job should have permission to run
 * before running it.
 *
 * Reschedules itself to run when the next job should run.
 *
 * Uses distributed locks to synchronize between the nodes.
 */
public class ScheduledJobRunner implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(ScheduledJobRunner.class);

    private final AtomicBoolean running = new AtomicBoolean(false);

    private final ScheduledJobQueue queue;

    private final Object schedulingLock = new Object();

    private ScheduledFuture<?> future;

    private final Collection<ISchedulePolicy> schedulePolicies;

    private volatile long nextScheduledRunTime = -1;

    public ScheduledJobRunner(ScheduledJobQueue queue, Collection<ISchedulePolicy> policies)
    {
        this.queue = queue;
        schedulePolicies = policies;
    }

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
            ScheduledJob next = queue.peek(this::fullCompare, this::validator);

            logger.debug("Next job: {}", next);

            if (next != null && next.isEnabled() && next.couldRunNow())
            {
                delay = runOrDelay(next);
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

    public void stop()
    {
        if (future != null)
        {
            future.cancel(false);
        }
    }

    private long runOrDelay(ScheduledJob job)
    {
        long delay = delayForJob(job);

        if (delay == -1)
        {
            try (Lock lock = job.getLock())
            {
                runJob(job);
            }
            catch (LockException | IOException e)
            {
                if (e.getCause() != null)
                    logger.warn("Unable to get schedule lock", e);
                return DistributedLock.getLockTime();
            }
        }

        return delay;
    }

    private void runJob(ScheduledJob job)
    {
        try
        {
            job.execute();

            if (job.shouldRunOnce())
                queue.remove(job);
            else
                queue.reQueue(job);
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

            ScheduledFuture<?> oldFuture = future;
            future = ScheduledExecutors.scheduledLongTasks.schedule(this, delay,
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
        ScheduledJob nextJob = queue.peek(this::fullCompare, this::validator);

        if (nextJob != null)
        {
            long delay = delayForJob(nextJob);

            if (delay != -1 && nextJob.isEnabled())
                return nextJob.timeToNextRun();
            else
                return 0;
        }

        return -1;
    }

    private boolean validator(ScheduledJob job)
    {
        return delayForJob(job) == -1;
    }

    /**
     * Returns the delay for the job if any schedule policies exists.
     *
     * @param job
     * @return The delay in milliseconds or -1 if it shouldn't be delayed.
     */
    private long delayForJob(ScheduledJob job)
    {
        if (schedulePolicies == null)
            return -1;

        return schedulePolicies.stream().mapToLong(policy -> policy.validate(job)).max().orElse(-1);
    }

    private int fullCompare(ScheduledJob j1, ScheduledJob j2)
    {
        if (!j1.isEnabled())
            return 1;
        if (!j2.isEnabled())
            return -1;

        if (!j1.couldRunNow())
            return 1;
        if (!j2.couldRunNow())
            return -1;

        long j1Delay = delayForJob(j1);
        long j2Delay = delayForJob(j2);

        if (j1Delay != j2Delay)
        {
            if (j1Delay == -1)
                return -1;
            else if (j2Delay == -1)
                return 1;
            else
                return Long.compare(j1Delay, j2Delay);
        }

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
