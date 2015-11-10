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

import java.util.*;
import java.util.function.Predicate;

import org.apache.cassandra.scheduling.JobConfiguration.BasePriority;
import org.apache.cassandra.utils.AbstractIterator;

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
    private final Map<BasePriority, List<ScheduledJob>> jobQueues = new HashMap<>();

    private final Object lock = new Object();

    ScheduledJobQueue()
    {
        for (BasePriority basePriority : BasePriority.values())
        {
            jobQueues.put(basePriority, new LinkedList<ScheduledJob>());
        }
    }

    /**
     * Add a job to the queue.
     *
     * @param job
     *            The job to add.
     */
    public void add(ScheduledJob job, Comparator<ScheduledJob> comparator)
    {
        synchronized (lock)
        {
            replaceOrAddJob(job);
            Collections.sort(jobQueues.get(job.getConfiguration().getBasePriority()), comparator);
        }
    }

    /**
     * Add the specified jobs to the queue.
     *
     * @param jobs
     *            The jobs to add.
     */
    public void addAll(Collection<? extends ScheduledJob> jobs, Comparator<ScheduledJob> comparator)
    {
        if (jobs.isEmpty())
            return;

        synchronized (lock)
        {
            jobs.forEach(job -> replaceOrAddJob(job));

            jobQueues.values().forEach(list -> Collections.sort(list, comparator));
        }
    }

    /**
     * Update all jobs and remove the old jobs.
     */
    public void updateJobs()
    {
        synchronized (lock)
        {
            jobQueues.values().forEach(list -> list.removeIf(j -> !j.update()));
        }
    }

    public void reQueue(ScheduledJob job)
    {
        synchronized (lock)
        {
            jobQueues.values().forEach(list -> list.remove(job));

            jobQueues.get(job.getConfiguration().getBasePriority()).add(job);
        }
    }

    /**
     * Get the highest prioritized job in the queue without removing it.
     *
     * @param comparator
     *            The comparator to use when sorting the scheduled jobs.
     * @return The highest prioritized job or null if no job is in the queue.
     */
    public ScheduledJob peek(Comparator<ScheduledJob> comparator, Predicate<ScheduledJob> validator)
    {
        synchronized (lock)
        {
            if (!jobQueues.isEmpty())
            {
                Iterator<ScheduledJob> it = new JobMergeIterator(comparator);

                ScheduledJob first = null;

                while (it.hasNext())
                {
                    ScheduledJob job = it.next();

                    if (!job.update())
                    {
                        it.remove();
                    }
                    else if (validator.test(job) && job.isEnabled() && job.couldRunNow())
                    {
                        return job;
                    }
                    else if (first == null)
                    {
                        first = job;
                    }
                }

                return first;
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
            jobQueues.values().forEach(list -> list.remove(job));
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

        jobQueues.values().forEach(list -> list.remove(newJob));

        jobQueues.get(newJob.getConfiguration().getBasePriority()).add(newJob);
    }

    private class JobMergeIterator extends AbstractIterator<ScheduledJob>
    {
        private final List<Iterator<ScheduledJob>> iterators;

        private final List<ScheduledJob> jobs;

        private final Comparator<ScheduledJob> comparator;

        private Iterator<ScheduledJob> lastIterator;

        private JobMergeIterator(Comparator<ScheduledJob> comp)
        {
            iterators = new ArrayList<>();
            jobs = new ArrayList<>();

            for (List<ScheduledJob> jobList : jobQueues.values())
            {
                iterators.add(jobList.iterator());
                jobs.add(null);
            }

            comparator = comp;
        }

        @Override
        protected ScheduledJob computeNext()
        {
            for (int i = 0; i < jobs.size(); i++)
            {
                if (jobs.get(i) == null)
                {
                    if (iterators.get(i).hasNext())
                    {
                        jobs.set(i, iterators.get(i).next());
                    }
                }
            }

            return collectNext();
        }

        private ScheduledJob collectNext()
        {
            ScheduledJob maxJob = null;

            for (int i = 0; i < jobs.size(); i++)
            {
                ScheduledJob job = jobs.get(i);
                if (maxJob == null || (job != null && comparator.compare(job, maxJob) < 0))
                {
                    maxJob = job;
                    lastIterator = iterators.get(i);
                }
            }

            return maxJob != null ? maxJob : endOfData();
        }

        public void remove()
        {
            if (lastIterator != null)
            {
                lastIterator.remove();
                lastIterator = null;
            }
        }

    }
}
