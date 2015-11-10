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
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.SystemDistributedKeyspace;
import org.apache.cassandra.repair.SystemDistributedKeyspace.JobState;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.scheduling.DistributedLock.Lock;
import org.apache.cassandra.scheduling.DistributedLock.LockException;
import org.apache.cassandra.scheduling.JobConfiguration.BasePriority;
import org.apache.cassandra.schema.RepairSchedulingParams;
import org.apache.cassandra.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A repair job is used to repair a certain table and consists of several {@link ScheduledRepairTask} that each repairs
 * a specific range for that table.
 */
public class ScheduledRepairJob extends ScheduledJob
{
    private static final String toStringFormat = "Repair %s.%s";

    public static IVersionedSerializer<ScheduledJob> serializer = new ScheduledRepairJobSerializer();

    private static final Logger logger = LoggerFactory.getLogger(ScheduledRepairJob.class);

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final Collection<ScheduledRepairTask> tasks;

    private final String keyspace;
    private final String table;
    private final RepairSchedulingParams params;
    private final boolean userDefined;

    private volatile UUID lastRepairId;

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

        option.getRanges().forEach(range -> tasks.add(new ScheduledRepairTask.Builder()
                        .withKeyspace(keyspace)
                        .withTable(table)
                        .withRange(range)
                        .withParams(params)
                        .build()));

        JobConfiguration.Builder configBuilder = new JobConfiguration.Builder()
                .withMinimumDelay(0).withEnabled(true)
                .withRunOnce(true)
                .withPriority(scheduledHigh ? BasePriority.HIGHEST : BasePriority.HIGH);

        Builder builder = new Builder()
                .withKeyspace(keyspace)
                .withTable(table)
                .withTasks(tasks)
                .withRepairParams(params)
                .withConfiguration(configBuilder.build())
                .userDefined(true);

        return builder.build();
    }

    /**
     * Create a new repair job with the provided parameters.
     *
     * @param configuration
     *            The job configuartion.
     * @param keyspace
     *            The keyspace this job repairs.
     * @param table
     *            The table this job repairs.
     * @param tasks
     *            The tasks assigned to this repair job.
     * @param params
     *            The repair configuration.
     * @param userDefined
     *            If the job was user defined
     * @param lastRepairId
     *            The last repair id that was used to determine when the ranges has been repaired.
     */
    private ScheduledRepairJob(JobConfiguration configuration,
            String keyspace,
            String table,
            Collection<ScheduledRepairTask> tasks,
            RepairSchedulingParams params,
            boolean userDefined,
            UUID lastRepairId)
    {
        super(configuration);
        this.keyspace = keyspace;
        this.table = table;
        this.tasks = new ArrayList<>(tasks);
        this.params = params;
        this.userDefined = userDefined;
        this.lastRepairId = lastRepairId;
    }

    @Override
    protected Collection<? extends ScheduledTask> getTasks()
    {
        try
        {
            lock.readLock().lock();
            return new ArrayList<>(this.tasks);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    @Override
    public boolean update()
    {
        if (userDefined)
        {
            return true;
        }

        if (getConfiguration().runOnce())
            return couldRunNow();

        try
        {
            Keyspace.open(keyspace).getColumnFamilyStore(table);

            JobConfiguration config = getConfiguration();
            JobConfiguration newConfig = reloadConfiguration();

            if (!newConfig.equals(config))
                setConfiguration(newConfig);

            updateTasks();
        }
        catch (IllegalArgumentException e)
        {
            logger.debug("Unable to reload repair job configuration", e);
            return false;
        }

        return true;
    }

    @Override
    public boolean couldRunNow()
    {
        if (userDefined)
        {
            return true;
        }

        List<Range<Token>> ranges = getRanges();

        long hintWindow = getActualHintWindow(Schema.instance.getCFMetaData(keyspace, table));

        long minTimeSinceLastRepair = System.currentTimeMillis() - hintWindow;

        RepairHistory repairHistory = getRepairHistory();

        lastRepairId = repairHistory.getLatestRepairId();

        for (Range<Token> range : ranges)
        {
            Long repaired = repairHistory.get(range);
            if (repaired == null || repaired < minTimeSinceLastRepair)
            {
                return true;
            }
        }

        return super.couldRunNow();
    }

    @Override
    public String toString()
    {
        return String.format(toStringFormat, keyspace, table);
    }

    @Override
    public Lock getLock() throws LockException
    {
        return DistributedLock.instance.tryGetLock(ScheduleManager.SCHEDULE_LOCK + "_repair", getPriority());
    }

    @Override
    public IVersionedSerializer<ScheduledJob> getSerializer()
    {
        return serializer;
    }

    /**
     * Get the actual hint window of a column family.
     *
     * @param cfMetaData
     * @return
     */
    public static long getActualHintWindow(CFMetaData cfMetaData)
    {
        long maxHintWindow = DatabaseDescriptor.getMaxHintWindow();
        long cfGcGraceSeconds = cfMetaData.params.gcGraceSeconds * 1000;

        return cfGcGraceSeconds < maxHintWindow ? cfGcGraceSeconds : maxHintWindow;
    }

    /**
     * Get the repair history of the scheduled repairs.
     * <p>
     * This retrieves the repair history of what repairs this node has performed.
     *
     * @param keyspace
     * @param table
     * @param ranges
     * @return
     */
    public static RepairHistory getRepairHistory(String keyspace, String table, Collection<Range<Token>> ranges)
    {
        return getRepairHistory(keyspace, table, ranges, null);
    }

    /**
     * Get the repair history of the scheduled repairs newer than the UUID provided.
     * <p>
     * This retrieves the repair history of what repairs this node has performed.
     *
     * @param keyspace
     * @param table
     * @param ranges
     * @param lastId
     *            the job id from the previously retrieved repair history or null.
     * @return
     * @see RepairHistory#getLatestRepairId()
     */
    public static RepairHistory getRepairHistory(String keyspace, String table, Collection<Range<Token>> ranges,
            UUID lastId)
    {
        return getRepairHistory(keyspace, table, DatabaseDescriptor.getBroadcastAddress(), ranges, lastId);
    }

    /**
     * Get the repair history of the scheduled repairs newer than the UUID provided.
     * <p>
     * This retrieves the repair history of what repairs the provided node has performed.
     *
     * @param keyspace
     * @param table
     * @param node
     *            the node to retrieve the history for.
     * @param ranges
     * @param lastId
     *            the latest job id from the previously retrieved repair history or null.
     * @return
     * @see RepairHistory#getLatestRepairId()
     */
    public static RepairHistory getRepairHistory(String keyspace, String table, InetAddress node,
            Collection<Range<Token>> ranges,
            UUID lastId)
    {
        RepairHistory repairHistory = new RepairHistory(keyspace, table, ranges);

        try
        {
            UntypedResultSet repairHistoryResultSet = SystemDistributedKeyspace.getScheduledJob(
                    String.format(toStringFormat, keyspace, table), node);

            Iterator<UntypedResultSet.Row> it = repairHistoryResultSet.iterator();

            UUID id = null;

            while (it.hasNext() && !repairHistory.isFull())
            {
                UntypedResultSet.Row row = it.next();

                id = row.getUUID("job_id");

                if (lastId != null && lastId.equals(id))
                {
                    break;
                }

                repairHistory.setLastIdIfUnset(id);

                String status = row.getString("status");

                if (!JobState.SUCCESS.toString().equals(status))
                {
                    continue;
                }

                String task_name = row.getString("task_name");

                assert task_name != null;

                Range<Token> range = ScheduledRepairTask.fromString(task_name);

                if (repairHistory.has(range) || !ranges.contains(range))
                {
                    continue;
                }

                if (row.has("finished_at"))
                {
                    Date finished_at = row.getTimestamp("finished_at");
                    repairHistory.tryAddRange(range, finished_at.getTime());
                }
            }
        }
        catch (Throwable t)
        {
            logger.error("Unable to get repair history for table {}.{}: {}", keyspace, table, t);
        }

        return repairHistory;
    }

    /**
     * A utility class used to keep track of the last time token ranges were repaired for a certain table and the repair
     * job id from the latest scheduled repair job.
     */
    public static class RepairHistory
    {
        private final String keyspace;
        private final String table;
        private final Collection<Range<Token>> ranges;

        private final Map<Range<Token>, Long> lastRepaired = new HashMap<>();

        private UUID lastId;

        private RepairHistory(String keyspace, String table, Collection<Range<Token>> ranges)
        {
            this.keyspace = keyspace;
            this.table = table;
            this.ranges = ranges;
        }

        /**
         * @return the keyspace the repairs was performed on.
         */
        public String getKeyspace()
        {
            return keyspace;
        }

        /**
         * @return the table the repairs was performed on.
         */
        public String getTable()
        {
            return table;
        }

        /**
         * @return the job id of the latest repair retrieved.
         */
        public UUID getLatestRepairId()
        {
            return this.lastId;
        }

        /**
         * @param range
         * @return the last time the provided range was repaired or null.
         */
        public Long get(Range<Token> range)
        {
            return lastRepaired.get(range);
        }

        /**
         * @param range
         * @return if there is any repair history for the provided range.
         */
        public boolean has(Range<Token> range)
        {
            return lastRepaired.containsKey(range);
        }

        private void setLastIdIfUnset(UUID lastId)
        {
            if (lastId == null)
            {
                this.lastId = lastId;
            }
        }

        private void tryAddRange(Range<Token> range, long repairedAt)
        {
            Long lastRepairedAt = lastRepaired.get(range);

            if (lastRepairedAt != null)
            {
                if (Long.compare(lastRepairedAt, repairedAt) < 0)
                {
                    lastRepaired.put(range, repairedAt);
                }
            }
            else
            {
                lastRepaired.put(range, repairedAt);
            }
        }

        private boolean isFull()
        {
            return ranges.size() <= lastRepaired.size();
        }
    }

    /**
     * Helper class used to create a {@link ScheduledRepairJob}.
     */
    public static class Builder
    {
        private String keyspace;
        private String table;
        private Collection<ScheduledRepairTask> tasks;
        private RepairSchedulingParams params = RepairSchedulingParams.DEFAULT;
        private JobConfiguration configuration;
        private boolean userDefined = false;
        private UUID lastRepairId = null;

        public Builder()
        {
        }

        /**
         * Set the keyspace for the {@link ScheduledRepairJob}.
         *
         * @param keyspace
         * @return this
         */
        public Builder withKeyspace(String keyspace)
        {
            this.keyspace = keyspace;
            return this;
        }

        /**
         * Set the table for the {@link ScheduledRepairJob}.
         *
         * @param table
         * @return this
         */
        public Builder withTable(String table)
        {
            this.table = table;
            return this;
        }

        /**
         * Set the {@link RepairSchedulingParams} for the {@link ScheduledRepairJob}.
         *
         * @param params
         * @return this
         */
        public Builder withRepairParams(RepairSchedulingParams params)
        {
            this.params = params;
            return this;
        }

        /**
         * Set the list of tasks for the {@link ScheduledRepairJob}.
         *
         * @param tasks
         * @return this
         */
        public Builder withTasks(Collection<ScheduledRepairTask> tasks)
        {
            this.tasks = tasks;
            return this;
        }

        /**
         * Set the configuration for the {@link ScheduledRepairJob}.
         *
         * @param configuration
         * @return this
         */
        public Builder withConfiguration(JobConfiguration configuration)
        {
            this.configuration = configuration;
            return this;
        }

        /**
         * Set the configuration for if the job is user defined.
         *
         * @param userDefined
         * @return this
         */
        public Builder userDefined(boolean userDefined)
        {
            this.userDefined = userDefined;
            return this;
        }

        /**
         * Set the last repair id that was used to determine when ranges has been repaired.
         *
         * @param lastRepairId
         * @return this
         */
        public Builder withLastRepairId(UUID lastRepairId)
        {
            this.lastRepairId = lastRepairId;
            return this;
        }

        /**
         * Build the {@link ScheduledRepairJob}.
         *
         * @return A {@link ScheduledRepairJob} based on the attributes specified.
         */
        public ScheduledRepairJob build()
        {
            assert keyspace != null && table != null && configuration != null && tasks != null;

            return new ScheduledRepairJob(configuration, keyspace, table, tasks, params, userDefined, lastRepairId);
        }
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
            job.serializeTasks(out, version);
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

                tasks.add(new ScheduledRepairTask.Builder()
                        .withKeyspace(keyspace)
                        .withTable(table)
                        .withRange(range)
                        .withParams(params)
                        .build());
            }

            return new ScheduledRepairJob(configuration, keyspace, table, tasks, params, false, null);
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
            size += job.serializedTasksSize(version);

            return size;
        }

    }

    private void updateTasks()
    {
        Collection<Range<Token>> taskRanges = getRanges();

        Collection<Range<Token>> actualRanges = StorageService.instance.getPrimaryRanges(keyspace);

        if (taskRanges.size() != actualRanges.size() || !taskRanges.containsAll(actualRanges))
        {
            lock.writeLock().lock();
            try
            {
                tasks.clear();

                actualRanges.forEach(range -> tasks.add(new ScheduledRepairTask.Builder()
                        .withKeyspace(keyspace)
                        .withTable(table)
                        .withRange(range)
                        .withParams(params)
                        .build()));
            }
            finally
            {
                lock.writeLock().unlock();
            }
        }

        updateLastRepaired();
    }

    private void updateLastRepaired()
    {
        RepairHistory repairHistory = getRepairHistory();

        lastRepairId = repairHistory.getLatestRepairId();

        lock.readLock().lock();
        try
        {
            for (ScheduledRepairTask task : tasks)
            {
                Long lastRepaired = repairHistory.get(task.getRepairRange());

                if (lastRepaired != null)
                {
                    if (task.getLastRunTime() < lastRepaired)
                    {
                        task.setLastRunTime(lastRepaired);
                    }
                }
            }
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    private List<Range<Token>> getRanges()
    {
        lock.readLock().lock();
        try
        {
            return tasks.stream().map(task -> task.getRepairRange()).collect(Collectors.toList());
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    private RepairHistory getRepairHistory()
    {
        return getRepairHistory(keyspace, table, getRanges(), lastRepairId);
    }

    private JobConfiguration reloadConfiguration()
    {
        CFMetaData cfMetaData = Schema.instance.getCFMetaData(keyspace, table);
        assert cfMetaData != null;

        RepairSchedulingParams params = cfMetaData.params.repairScheduling;

        return new JobConfiguration.Builder()
                .withMinimumDelay(params.minDelay())
                .withPriority(BasePriority.LOW)
                .withEnabled(params.enabled())
                .build();
    }

    private void serializeTasks(DataOutputPlus out, int version) throws IOException
    {
        lock.readLock().lock();
        try
        {
            out.writeInt(tasks.size());
            for (ScheduledRepairTask task : tasks)
            {
                AbstractBounds.tokenSerializer.serialize(task.getRepairRange(), out, version);
            }
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    private long serializedTasksSize(int version)
    {
        long size = 0;

        lock.readLock().lock();
        try
        {
            size += TypeSizes.sizeof(tasks.size());
            for (ScheduledRepairTask task : tasks)
            {
                size += AbstractBounds.tokenSerializer.serializedSize(task.getRepairRange(), version);
            }
        }
        finally
        {
            lock.readLock().unlock();
        }

        return size;
    }
}
