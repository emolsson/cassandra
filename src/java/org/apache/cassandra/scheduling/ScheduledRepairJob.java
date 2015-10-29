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
import java.util.ArrayList;
import java.util.Collection;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.scheduling.DistributedLock.Lock;
import org.apache.cassandra.scheduling.DistributedLock.LockException;
import org.apache.cassandra.scheduling.JobConfiguration.BasePriority;
import org.apache.cassandra.schema.RepairSchedulingParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A repair job is used to repair a certain table and consists of several {@link ScheduledRepairTask} that each repairs
 * a specific range for that table.
 */
public class ScheduledRepairJob extends ScheduledJob
{
    public static IVersionedSerializer<ScheduledJob> serializer = new ScheduledRepairJobSerializer();

    private static final Logger logger = LoggerFactory.getLogger(ScheduledRepairJob.class);

    private final Collection<ScheduledRepairTask> tasks;

    private final String keyspace;
    private final String table;
    private final RepairSchedulingParams params;

    /**
     * Create a new repair job based on the provided parameters.
     *
     * <br>
     *
     * <br>
     *
     * To actually schedule the job it either has to be created through an {@link IScheduler} or by invoking the method
     * {@link ScheduleManager#schedule(ScheduledJob)}
     *
     * @param keyspace
     *            The keyspace to repair.
     * @param table
     *            The table to repair.
     * @param option
     *            The repair options to use.
     * @param scheduledHigh
     *            True if the job should run as soon as possible.
     * @return The created repair job.
     */
    public static ScheduledRepairJob fromRepairOptions(String keyspace, String table, RepairOption option,
            boolean scheduledHigh)
    {
        Collection<ScheduledRepairTask> tasks = new ArrayList<>();

        RepairSchedulingParams params = new RepairSchedulingParams(true, option.isIncremental(), 0,
                option.getParallelism());

        option.getRanges().forEach(range -> tasks.add(new ScheduledRepairTask(keyspace, table, range, params)));

        if (scheduledHigh)
            return new ScheduledRepairJob(keyspace, table, tasks, params, true, BasePriority.HIGHEST);
        else
            return new ScheduledRepairJob(keyspace, table, tasks, params, true, BasePriority.HIGH);
    }

    public ScheduledRepairJob(String keyspace, String table, Collection<ScheduledRepairTask> tasks,
            RepairSchedulingParams params)
    {
        this(keyspace, table, tasks, params, false, BasePriority.LOW);
    }

    private ScheduledRepairJob(String keyspace, String table, Collection<ScheduledRepairTask> tasks,
            RepairSchedulingParams params, boolean runOnce, BasePriority priority)
    {
        this(new JobConfiguration(params.minDelay(), priority, params.enabled(), runOnce), keyspace, table, tasks,
                params);
    }

    public ScheduledRepairJob(JobConfiguration configuration, String keyspace, String table,
            Collection<ScheduledRepairTask> tasks, RepairSchedulingParams params)
    {
        super(configuration);
        this.keyspace = keyspace;
        this.table = table;
        this.tasks = new ArrayList<>(tasks);
        this.params = params;
    }

    @Override
    protected Collection<? extends ScheduledTask> getTasks()
    {
        return this.tasks;
    }

    @Override
    public boolean update()
    {
        try
        {
            Keyspace.open(keyspace).getColumnFamilyStore(table);

            JobConfiguration config = getConfiguration();
            JobConfiguration newConfig = reloadConfiguration();

            if (!newConfig.equals(config))
                setConfiguration(newConfig);
        }
        catch (IllegalArgumentException e)
        {
            logger.debug("Unable to reload repair job configuration", e);
            return false;
        }

        return true;
    }

    private JobConfiguration reloadConfiguration()
    {
        CFMetaData cfMetaData = Schema.instance.getCFMetaData(keyspace, table);
        assert cfMetaData != null;

        RepairSchedulingParams params = cfMetaData.params.repairScheduling;

        return new JobConfiguration(params.minDelay(), BasePriority.LOW, params.enabled());
    }

    @Override
    public String toString()
    {
        return String.format("Scheduled repair of %s.%s", keyspace, table);
    }

    @Override
    public Lock getLock() throws LockException
    {
        return DistributedLock.instance.tryGetLock(ScheduleManager.SCHEDULE_LOCK + "_repair", getPriority());
    }

    public IVersionedSerializer<ScheduledJob> getSerializer()
    {
        return serializer;
    }

    public static class ScheduledRepairJobSerializer implements IVersionedSerializer<ScheduledJob>
    {

        @Override
        public void serialize(ScheduledJob t, DataOutputPlus out, int version) throws IOException
        {
            assert t instanceof ScheduledRepairJob;
            ScheduledRepairJob job = (ScheduledRepairJob) t;

            JobConfiguration.serializer.serialize(t.getConfiguration(), out, version);
            out.writeUTF(job.keyspace);
            out.writeUTF(job.table);
            RepairSchedulingParams.serializer.serialize(job.params, out, version);
            out.writeInt(job.tasks.size());
            for (ScheduledRepairTask task : job.tasks)
            {
                AbstractBounds.tokenSerializer.serialize(task.getRepairRange(), out, version);
            }
        }

        @Override
        public ScheduledJob deserialize(DataInputPlus in, int version) throws IOException
        {
            JobConfiguration configuration = JobConfiguration.serializer.deserialize(in, version);
            String keyspace = in.readUTF();
            String table = in.readUTF();
            RepairSchedulingParams params = RepairSchedulingParams.serializer.deserialize(in, version);
            int nTasks = in.readInt();
            Collection<ScheduledRepairTask> tasks = new ArrayList<>();
            for (int i = 0; i < nTasks; i++)
            {
                Range<Token> range = (Range<Token>) AbstractBounds.tokenSerializer.deserialize(in,
                        MessagingService.globalPartitioner(), version);
                tasks.add(new ScheduledRepairTask(keyspace, table, range, params));
            }

            return new ScheduledRepairJob(configuration, keyspace, table, tasks, params);
        }

        @Override
        public long serializedSize(ScheduledJob t, int version)
        {
            assert t instanceof ScheduledRepairJob;
            ScheduledRepairJob job = (ScheduledRepairJob) t;

            long size = JobConfiguration.serializer.serializedSize(t.getConfiguration(), version);
            size += TypeSizes.sizeof(job.keyspace);
            size += TypeSizes.sizeof(job.table);
            size += RepairSchedulingParams.serializer.serializedSize(job.params, version);
            size += TypeSizes.sizeof(job.tasks.size());
            for (ScheduledRepairTask task : job.tasks)
            {
                size += AbstractBounds.tokenSerializer.serializedSize(task.getRepairRange(), version);
            }

            return size;
        }

    }
}
