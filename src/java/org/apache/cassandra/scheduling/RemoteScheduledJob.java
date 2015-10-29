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

import java.net.InetAddress;
import java.util.*;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.scheduling.DistributedLock.Lock;
import org.apache.cassandra.scheduling.DistributedLock.LockException;
import org.apache.cassandra.scheduling.JobConfiguration.BasePriority;

public class RemoteScheduledJob extends ScheduledJob
{
    public static RemoteScheduledJob createJob(InetAddress endpoint, List<ScheduledJob> jobs)
    {
        Collection<RemoteScheduledRepairTask> tasks = new ArrayList<>();

        jobs.forEach(job -> tasks.add(new RemoteScheduledRepairTask(endpoint, job)));

        JobConfiguration configuration = new JobConfiguration(3600, BasePriority.HIGHEST, true, true);

        return new RemoteScheduledJob(configuration, tasks, endpoint, jobs);
    }

    private Collection<RemoteScheduledRepairTask> tasks;

    private InetAddress endpoint;
    private final List<ScheduledJob> jobs;

    /**
     * @param configuration
     */
    private RemoteScheduledJob(JobConfiguration configuration, Collection<RemoteScheduledRepairTask> tasks,
            InetAddress endpoint, List<ScheduledJob> jobs)
    {
        super(configuration);

        this.endpoint = endpoint;
        this.jobs = new ArrayList<>(jobs);
        this.tasks = new ArrayList<>(tasks);
    }

    @Override
    public Collection<? extends ScheduledTask> getTasks()
    {
        return tasks;
    }

    @Override
    public boolean update()
    {
        return true;
    }

    @Override
    public String toString()
    {
        return String.format("Scheduled remote job(s) for %s (%s)", endpoint, jobs);
    }

    @Override
    public Lock getLock() throws LockException
    {
        return DistributedLock.instance.tryGetLock(ScheduleManager.SCHEDULE_LOCK + "_remote", getPriority());
    }

    private static class RemoteScheduledRepairTask extends ScheduledTask
    {
        private final InetAddress endpoint;

        private final ScheduledJob job;

        public RemoteScheduledRepairTask(InetAddress endpoint, ScheduledJob job)
        {
            this.endpoint = endpoint;
            this.job = job;
        }

        @Override
        public boolean execute()
        {
            MessagingService.instance()
                    .sendOneWay(new MessageOut<>(MessagingService.Verb.SCHEDULED_JOB, job, ScheduledJob.serializer),
                            endpoint);
            return true;
        }

    }

    @Override
    public IVersionedSerializer<ScheduledJob> getSerializer()
    {
        throw new UnsupportedOperationException("RemoteScheduledRepairJob shouldn't be serialized");
    }
}
