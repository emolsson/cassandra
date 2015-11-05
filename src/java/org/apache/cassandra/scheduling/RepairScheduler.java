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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.repair.SystemDistributedKeyspace;
import org.apache.cassandra.repair.SystemDistributedKeyspace.RepairState;
import org.apache.cassandra.scheduling.JobConfiguration.BasePriority;
import org.apache.cassandra.schema.RepairSchedulingParams;
import org.apache.cassandra.service.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The default repair scheduler used to create repair jobs.
 */
public class RepairScheduler extends MigrationListener implements IScheduler, IEndpointLifecycleSubscriber
{
    private static final Logger logger = LoggerFactory.getLogger(RepairScheduler.class);

    private final ConcurrentMap<InetAddress, Long> endpointMap = new ConcurrentHashMap<>();

    private final Set<String> scheduledTables = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

    public RepairScheduler()
    {
        MigrationManager.instance.register(this);
    }

    @Override
    public Collection<ScheduledJob> createNewJobs()
    {
        if (!StorageService.instance.isJoined())
            return Collections.emptyList();

        Collection<ScheduledJob> ret = new ArrayList<>();

        ScheduleableTableIterator it = new ScheduleableTableIterator();

        while (it.hasNext())
        {
            CFMetaData cfMetaData = it.next();

            Collection<Range<Token>> ranges = StorageService.instance.getPrimaryRanges(cfMetaData.ksName);

            ScheduledJob job = maybeGetTableJob(cfMetaData, ranges);
            if (job != null)
            {
                ret.add(job);
                logger.info("Added scheduled repair job for {}.{}", cfMetaData.ksName, cfMetaData.cfName);
            }
        }

        return ret;
    }

    private ScheduledJob maybeGetTableJob(CFMetaData cfMetaData, Collection<Range<Token>> ranges)
    {
        String keyspace = cfMetaData.ksName;
        String table = cfMetaData.cfName;

        String ksTb = keyspace + "." + table;
        if (scheduledTables.add(ksTb))
        {
            try
            {
                return new ScheduledRepairJob.Builder()
                        .withKeyspace(keyspace)
                        .withTable(table)
                        .withTasks(createRepairTasks(cfMetaData, ranges))
                        .withRepairParams(cfMetaData.params.repairScheduling)
                        .build();
            }
            catch (ConfigurationException e)
            {
                scheduledTables.remove(ksTb);
                logger.warn("Unable to add scheduled job for " + ksTb + " ", e);
            }
        }

        return null;
    }

    private Collection<ScheduledRepairTask> createRepairTasks(CFMetaData cfMetaData, Collection<Range<Token>> ranges)
    {
        final String keyspace = cfMetaData.ksName;
        final String table = cfMetaData.cfName;
        final RepairSchedulingParams params = cfMetaData.params.repairScheduling;

        Collection<ScheduledRepairTask> tasks = new ArrayList<>();

        Map<Range<Token>, Long> lastRepairedAt = getLastRepairedForRanges(keyspace, table, ranges);

        for (Range<Token> range : ranges)
        {
            ScheduledRepairTask.Builder builder = new ScheduledRepairTask.Builder()
                    .withKeyspace(keyspace)
                    .withTable(table)
                    .withRange(range)
                    .withParams(params);

            if (lastRepairedAt.containsKey(range))
                builder.withLastRepairedAt(lastRepairedAt.get(range));

            tasks.add(builder.build());
        }

        return tasks;
    }

    @Override
    public void onCreateColumnFamily(String ksName, String cfName)
    {
        ScheduleManager.instance.forceUpdate();
    }

    @Override
    public void onUpdateKeyspace(String ksName)
    {
        ScheduleManager.instance.forceUpdate();
    }

    @Override
    public void onUpdateColumnFamily(String ksName, String cfName, boolean columnsDidChange)
    {
        ScheduleManager.instance.forceUpdate();
    }

    @Override
    public void onDropKeyspace(String ksName)
    {
        ScheduleManager.instance.forceUpdate();

        String ksStart = ksName + ".";

        scheduledTables.removeIf(ksTb -> ksTb.startsWith(ksStart));
    }

    @Override
    public void onDropColumnFamily(String ksName, String cfName)
    {
        ScheduleManager.instance.forceUpdate();

        scheduledTables.remove(ksName + "." + cfName);
    }

    @Override
    public void onJoinCluster(InetAddress endpoint)
    {
        // Nothing to do
    }

    @Override
    public void onLeaveCluster(InetAddress endpoint)
    {
        // Nothing to do
    }

    @Override
    public void onUp(InetAddress endpoint)
    {
        Long downSince = endpointMap.remove(endpoint);

        if (downSince != null)
        {
            List<ScheduledJob> jobs = new ArrayList<>();
            long downTime = System.currentTimeMillis() - downSince;
            JobConfiguration configuration = new JobConfiguration.Builder()
                    .withMinimumDelay(3600)
                    .withPriority(BasePriority.HIGHEST)
                    .withEnabled(true)
                    .withRunOnce(true)
                    .build();

            ScheduleableTableIterator it = new ScheduleableTableIterator();

            long maxHintWindow = DatabaseDescriptor.getMaxHintWindow();

            while (it.hasNext())
            {
                CFMetaData cfMetaData = it.next();

                Collection<Range<Token>> ranges = StorageService.instance.getPrimaryRangesForEndpoint(
                        cfMetaData.ksName, endpoint);

                long cfGcGraceSeconds = cfMetaData.params.gcGraceSeconds * 1000;

                long minDownTime = cfGcGraceSeconds < maxHintWindow ? cfGcGraceSeconds : maxHintWindow;

                if (downTime >= minDownTime)
                {
                    String keyspace = cfMetaData.ksName;
                    String table = cfMetaData.cfName;


                    ScheduledRepairJob.Builder builder = new ScheduledRepairJob.Builder()
                            .withKeyspace(keyspace)
                            .withTable(table)
                            .withTasks(createRepairTasks(cfMetaData, ranges))
                            .withRepairParams(cfMetaData.params.repairScheduling)
                            .withConfiguration(configuration);

                    jobs.add(builder.build());
                }
            }

            if (!jobs.isEmpty())
            {
                ScheduleManager.instance.schedule(RemoteScheduledJob.createJob(endpoint, jobs));
                ScheduleManager.instance.forceUpdate();
            }
        }
    }

    @Override
    public void onDown(InetAddress endpoint)
    {
        endpointMap.put(endpoint, System.currentTimeMillis());
    }

    @Override
    public void onMove(InetAddress endpoint)
    {
        // Nothing to do
    }

    private class ScheduleableTableIterator implements Iterator<CFMetaData>
    {
        private final Iterator<String> keyspaceIterator;
        private Iterator<CFMetaData> cfIterator = null;

        private CFMetaData next;

        public ScheduleableTableIterator()
        {
            keyspaceIterator = new HashSet<>(Schema.instance.getKeyspaces()).iterator();
        }

        @Override
        public boolean hasNext()
        {
            CFMetaData nextData = nextFromCurrent();
            if (nextData == null)
            {
                while (nextData == null && keyspaceIterator.hasNext())
                {
                    Keyspace keyspace = Keyspace.open(keyspaceIterator.next());

                    if (keyspace.getReplicationStrategy().getReplicationFactor() < 2)
                    {
                        continue;
                    }

                    cfIterator = keyspace.getMetadata().tables.iterator();

                    nextData = nextFromCurrent();
                }
            }

            next = nextData;

            return next != null;
        }

        private CFMetaData nextFromCurrent()
        {
            if (cfIterator == null)
            {
                return null;
            }

            while (cfIterator.hasNext())
            {
                CFMetaData cfMetaData = cfIterator.next();

                if (cfMetaData.params.repairScheduling.enabled())
                {
                    return cfMetaData;
                }
            }

            return null;
        }

        @Override
        public CFMetaData next()
        {
            return next;
        }

    }

    private static Map<Range<Token>, Long> getLastRepairedForRanges(String keyspace, String table,
            Collection<Range<Token>> ranges)
    {
        Map<Range<Token>, Long> lastRepairedAt = new HashMap<>();
        IPartitioner partitioner = StorageService.instance.getTokenMetadata().partitioner;

        try
        {
            UntypedResultSet repairHistory = SystemDistributedKeyspace.getRepairJobs(keyspace, table);

            Iterator<UntypedResultSet.Row> it = repairHistory.iterator();

            while (it.hasNext() && lastRepairedAt.size() < ranges.size())
            {
                UntypedResultSet.Row row = it.next();

                String status = row.getString("status");

                if (!RepairState.SUCCESS.toString().equals(status))
                {
                    continue;
                }

                String range_begin = row.getString("range_begin");
                String range_end = row.getString("range_end");

                assert range_begin != null && range_end != null;

                Token start = partitioner.getTokenFactory().fromString(range_begin);
                Token end = partitioner.getTokenFactory().fromString(range_end);

                Range<Token> range = new Range<Token>(start, end);

                if (lastRepairedAt.containsKey(range) || !ranges.contains(range))
                {
                    continue;
                }

                if (row.has("finished_at"))
                {
                    Date finished_at = row.getTimestamp("finished_at");
                    lastRepairedAt.put(range, finished_at.getTime());
                }
            }
        }
        catch (Throwable t)
        {
            logger.error("Unable to get repair history for table {}.{}: {}", keyspace, table, t);
        }

        return lastRepairedAt;
    }
}
