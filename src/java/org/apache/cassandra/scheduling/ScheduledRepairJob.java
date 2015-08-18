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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.repair.messages.RepairOption;
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
    private static final Logger logger = LoggerFactory.getLogger(ScheduledRepairJob.class);

    private String keyspace;
    private String table;

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
        int ranges = option.getRanges().size();

        Collection<ScheduledRepairTask> tasks = new ArrayList<>(ranges + ranges / 3 + 1);

        RepairSchedulingParams params = new RepairSchedulingParams(true, option.isIncremental(), 0,
                option.getParallelism());

        for (Range<Token> range : option.getRanges())
        {
            tasks.add(new ScheduledRepairTask(keyspace, table, range, params));
        }
        if (scheduledHigh)
            return new ScheduledRepairJob(keyspace, table, tasks, params, true, BasePriority.HIGHEST);
        else
            return new ScheduledRepairJob(keyspace, table, tasks, params, true, BasePriority.HIGH);
    }

    public ScheduledRepairJob(String keyspace, String table, Collection<ScheduledRepairTask> tasks,
            RepairSchedulingParams params)
    {
        this(keyspace, table, tasks, params, false);
    }

    private ScheduledRepairJob(String keyspace, String table, Collection<ScheduledRepairTask> tasks,
            RepairSchedulingParams params, boolean runOnce)
    {
        this(keyspace, table, tasks, params, runOnce, BasePriority.LOW);
    }

    private ScheduledRepairJob(String keyspace, String table, Collection<ScheduledRepairTask> tasks,
            RepairSchedulingParams params, boolean runOnce, BasePriority priority)
    {
        super(new JobConfiguration(params.minDelay(), priority, params.enabled(), runOnce), tasks);
        this.keyspace = keyspace;
        this.table = table;
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
}
