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
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

/**
 * A queue for scheduled jobs that finds the job with highest priority when {@link ScheduledJobQueue#peek() peek()} is
 * called.
 *
 * <br>
 *
 * <br>
 *
 * This class is thread-safe.
 */
class ScheduledJobQueue
{
    private final List<ScheduledJob> myJobs = new LinkedList<>();

    private final ScheduledJobComparator myJobComparator = new ScheduledJobComparator();

    private final Object lock = new Object();

    ScheduledJobQueue()
    {
    }

    /**
     * Add a job to the queue.
     *
     * @param job
     *            The job to add.
     */
    public void add(ScheduledJob job)
    {
        synchronized (lock)
        {
            replaceOrAddJob(job);
        }
    }

    /**
     * Add the specified jobs to the queue.
     *
     * @param jobs
     *            The jobs to add.
     */
    public void addAll(Collection<? extends ScheduledJob> jobs)
    {
        synchronized (lock)
        {
            jobs.forEach(job -> replaceOrAddJob(job));
        }
    }

    /**
     * Update all jobs and remove the old jobs.
     */
    public void updateJobs()
    {
        synchronized (lock)
        {
            myJobs.removeIf(p -> !p.update());
        }
    }

    /**
     * Get the highest prioritized job in the queue without removing it.
     *
     * @return The highest prioritized job or null if no job is in the queue.
     */
    public ScheduledJob peek()
    {
        synchronized (lock)
        {
            if (!myJobs.isEmpty())
            {
                Collections.sort(myJobs, myJobComparator);

                return myJobs.get(0);
            }
        }

        return null;
    }

    /**
     * Remove the provided job from the queue.
     *
     * @param job
     */
    public void remove(ScheduledJob job)
    {
        assert job != null;

        synchronized (lock)
        {
            myJobs.remove(job);
        }
    }

    /**
     * Replace or add a job depending if it already exists in the queue.
     *
     * @param newJob
     *            The job to add or replace with.
     */
    private void replaceOrAddJob(ScheduledJob newJob)
    {
        assert newJob != null;

        myJobs.remove(newJob);

        myJobs.add(newJob);
    }

    /**
     * Comparator that sorts jobs based on their priority.
     */
    private class ScheduledJobComparator implements Comparator<ScheduledJob>
    {
        @Override
        public int compare(ScheduledJob o1, ScheduledJob o2)
        {
            if (!o1.couldRunNow())
                return 1;
            if (!o2.couldRunNow())
                return -1;

            int o1Prio = o1.getPriority();
            int o2Prio = o2.getPriority();

            if (o2Prio > o1Prio)
                return 1;
            else if (o1Prio > o2Prio)
                return -1;
            else
                return o1.toString().compareTo(o2.toString());
        }
    }
}
