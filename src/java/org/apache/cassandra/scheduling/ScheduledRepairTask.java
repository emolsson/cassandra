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

import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ScheduledExecutionException;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.schema.RepairSchedulingParams;
import org.apache.cassandra.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A repair task is used to repair a single range for a table.
 *
 * <br>
 *
 * <br>
 *
 * It is usually contained in a job such as {@link ScheduledRepairJob}.
 */
class ScheduledRepairTask extends ScheduledTask
{
    private static final Logger logger = LoggerFactory.getLogger(ScheduledRepairTask.class);

    private final String keyspace;
    private final String table;
    private final Range<Token> repairRange;
    private final RepairSchedulingParams params;

    /**
     * Create a new repair task with the provided parameters.
     *
     * @param lastRepairedAt
     *            The last time this task was run.
     * @param keyspace
     *            The keyspace to repair.
     * @param table
     *            The table to repair.
     * @param repairRange
     *            The range to repair.
     * @param params
     *            The repair configuration.
     */
    private ScheduledRepairTask(long lastRepairedAt,
            String keyspace,
            String table,
            Range<Token> repairRange,
            RepairSchedulingParams params)
    {
        super(lastRepairedAt);
        this.keyspace = keyspace;
        this.table = table;
        this.repairRange = repairRange;
        this.params = params;
    }

    /**
     * @return The range this task should repair.
     */
    public Range<Token> getRepairRange()
    {
        return repairRange;
    }

    @Override
    public void execute() throws ScheduledExecutionException
    {
        logger.debug("Running repair of {}.{} for the range {}", keyspace, table, repairRange);
        RepairOption option = RepairOption.parse(createRepairOptions(), StorageService.instance.getTokenMetadata().partitioner);

        option.getRanges().add(repairRange);

        try
        {
            StorageService.instance.forceRepairBlocking(keyspace, option);
            logger.debug("Repair of {}.{} for the range {} finished", keyspace, table, repairRange);
        }
        catch (Exception e)
        {
            logger.warn("Error while running repair for {}.{} of range {}", keyspace, table, repairRange, e);
            throw new ScheduledExecutionException(e);
        }
    }

    @Override
    public String toString()
    {
        return String.format("RepairTask %d,%d", repairRange.left, repairRange.right);
    }

    public static Range<Token> fromString(String task)
    {
        IPartitioner partitioner = StorageService.instance.getTokenMetadata().partitioner;
        String[] task_part = task.split(" ");
        assert task_part.length == 2;

        String[] tokens = task_part[1].split(",");
        assert tokens.length == 2;

        Token start = partitioner.getTokenFactory().fromString(tokens[0]);
        Token end = partitioner.getTokenFactory().fromString(tokens[1]);

        return new Range<Token>(start, end);
    }

    /**
     * Helper class used to create a {@link ScheduledRepairTask}.
     */
    public static class Builder
    {
        private String keyspace;
        private String table;
        private Range<Token> range;
        private RepairSchedulingParams params;
        private long lastRepairedAt = -1;

        public Builder()
        {

        }

        /**
         * Set the keyspace for the {@link ScheduledRepairTask}.
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
         * Set the table fpr the {@link ScheduledRepairTask}.
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
         * Set the range for the {@link ScheduledRepairTask}.
         *
         * @param range
         * @return this
         */
        public Builder withRange(Range<Token> range)
        {
            this.range = range;
            return this;
        }

        /**
         * Set the repair params for the {@link ScheduledRepairTask}.
         *
         * @param params
         * @return this
         */
        public Builder withParams(RepairSchedulingParams params)
        {
            this.params = params;
            return this;
        }

        /**
         * Set the last repaired flag for the {@link ScheduledRepairTask}.
         *
         * @param lastRepairedAt
         * @return this
         */
        public Builder withLastRepairedAt(long lastRepairedAt)
        {
            this.lastRepairedAt = lastRepairedAt;
            return this;
        }

        /**
         * Build the {@link ScheduledRepairTask}.
         *
         * @return A {@link ScheduledRepairTask} based on the attributes specified.
         */
        public ScheduledRepairTask build()
        {
            return new ScheduledRepairTask(lastRepairedAt, keyspace, table, range, params);
        }
    }

    private Map<String, String> createRepairOptions()
    {
        Map<String, String> ret = new HashMap<>();

        ret.put(RepairOption.PARALLELISM_KEY, params.parallelism().toString());
        ret.put(RepairOption.PRIMARY_RANGE_KEY, Boolean.toString(false));
        ret.put(RepairOption.INCREMENTAL_KEY, Boolean.toString(params.incremental()));
        ret.put(RepairOption.JOB_THREADS_KEY, Integer.toString(1));
        ret.put(RepairOption.TRACE_KEY, Boolean.toString(false));
        ret.put(RepairOption.COLUMNFAMILIES_KEY, table);

        return ret;
    }

}
