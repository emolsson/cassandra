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

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.UntypedResultSet.Row;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.SystemDistributedKeyspace;
import org.apache.cassandra.repair.SystemDistributedKeyspace.JobState;
import org.apache.cassandra.scheduling.DistributedLock.Lock;
import org.apache.cassandra.scheduling.DistributedLock.LockException;
import org.apache.cassandra.scheduling.JobConfiguration.BasePriority;

/**
 * A remote scheduled job is used to schedule a set of {@link ScheduledJob}.
 *
 * These jobs must implement the {@link ScheduledJob#getSerializer()} properly.
 */
public class RemoteScheduledJob extends ScheduledJob
{
    public static RemoteScheduledJob createJob(InetAddress endpoint, String uniqueId, List<ScheduledJob> jobs)
    {
        Collection<RemoteScheduledTask> tasks = new ArrayList<>();

        jobs.forEach(job -> tasks.add(new RemoteScheduledTask(endpoint, job)));

        JobConfiguration configuration = new JobConfiguration.Builder()
                .withMinimumDelay(3600)
                .withPriority(BasePriority.HIGHEST)
                .withEnabled(true)
                .withRunOnce(true)
                .build();

        return new RemoteScheduledJob(configuration, tasks, uniqueId, endpoint);
    }

    private Collection<RemoteScheduledTask> tasks;

    private InetAddress endpoint;

    private final String uniqueId;

    /**
     * Create a new remote scheduled job based on the parameters provided.
     *
     * @param configuration
     *            The job configuration for this job.
     * @param tasks
     *            The tasks to run.
     * @param uniqueId
     *            The unique id for the job, to reduce the number of nodes running this job.
     * @param endpoint
     *            The endpoint to send the scheduled jobs.
     */
    private RemoteScheduledJob(JobConfiguration configuration,
            Collection<RemoteScheduledTask> tasks,
            String uniqueId,
            InetAddress endpoint)
    {
        super(configuration);

        this.uniqueId = uniqueId;
        this.endpoint = endpoint;
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
        UntypedResultSet res = SystemDistributedKeyspace.getScheduledJob(toString(), endpoint);

        Iterator<Row> it = res.iterator();

        while (it.hasNext())
        {
            Row row = it.next();

            String taskName = row.getString("task_name");
            String status = row.getString("status");

            if (taskName != null && JobState.SUCCESS.equals(status))
            {
                tryRemoveTask(taskName);

                if (tasks.isEmpty())
                {
                    break;
                }
            }
        }

        if (tasks.isEmpty())
        {
            return false;
        }

        return true;
    }

    @Override
    public InetAddress getNode()
    {
        return endpoint;
    }

    @Override
    public String toString()
    {
        return String.format("Remote %s-%s", endpoint, uniqueId);
    }

    @Override
    public Lock getLock() throws LockException
    {
        return DistributedLock.instance.tryGetLock(ScheduleManager.SCHEDULE_LOCK + "_remote", getPriority());
    }

    @Override
    public IVersionedSerializer<ScheduledJob> getSerializer()
    {
        throw new UnsupportedOperationException("RemoteScheduledJob shouldn't be serialized");
    }

    private static class RemoteScheduledTask extends ScheduledTask
    {
        private final InetAddress endpoint;

        private final ScheduledJob job;

        public RemoteScheduledTask(InetAddress endpoint, ScheduledJob job)
        {
            this.endpoint = endpoint;
            this.job = job;
        }

        @Override
        public void execute()
        {
            MessagingService.instance()
                    .sendOneWay(new MessageOut<>(MessagingService.Verb.SCHEDULED_JOB, job, ScheduledJob.serializer),
                            endpoint);
        }

        @Override
        public String toString()
        {
            return String.format("RemoteScheduledTask-%s", job);
        }

    }

    private void tryRemoveTask(String taskName)
    {
        Iterator<RemoteScheduledTask> it = tasks.iterator();

        while (it.hasNext())
        {
            RemoteScheduledTask task = it.next();

            if (taskName.equals(task.toString()))
            {
                it.remove();
                break;
            }
        }
    }
}
