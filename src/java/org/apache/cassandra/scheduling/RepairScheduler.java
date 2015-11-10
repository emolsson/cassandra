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

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.scheduling.JobConfiguration.BasePriority;
import org.apache.cassandra.scheduling.ScheduledRepairJob.RepairHistory;
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

    private final Map<InetAddress, Long> downEndpoints = new ConcurrentHashMap<>();

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

            ScheduledJob job = maybeGetTableJob(cfMetaData);
            if (job != null)
            {
                ret.add(job);
                logger.info("Added scheduled repair job for {}.{}", cfMetaData.ksName, cfMetaData.cfName);
            }
        }

        return ret;
    }

    private ScheduledJob maybeGetTableJob(CFMetaData cfMetaData)
    {
        String keyspace = cfMetaData.ksName;
        String table = cfMetaData.cfName;

        String ksTb = keyspace + "." + table;
        if (scheduledTables.add(ksTb))
        {
            try
            {
                Collection<Range<Token>> ranges = StorageService.instance.getPrimaryRanges(cfMetaData.ksName);
                RepairHistory repairHistory = ScheduledRepairJob.getRepairHistory(keyspace, table, ranges);

                return new ScheduledRepairJob.Builder()
                        .withKeyspace(keyspace)
                        .withTable(table)
                        .withTasks(createRepairTasks(cfMetaData, ranges, repairHistory))
                        .withRepairParams(cfMetaData.params.repairScheduling)
                        .withLastRepairId(repairHistory.getLatestRepairId())
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

    private Collection<ScheduledRepairTask> createRepairTasksForEndpoint(CFMetaData cfMetaData, InetAddress endpoint)
    {
        return createRepairTasks(cfMetaData, StorageService.instance.getPrimaryRangesForEndpoint(cfMetaData.ksName, endpoint), null);
    }

    private Collection<ScheduledRepairTask> createRepairTasks(CFMetaData cfMetaData, Collection<Range<Token>> ranges,
            RepairHistory repairHistory)
    {
        final String keyspace = cfMetaData.ksName;
        final String table = cfMetaData.cfName;
        final RepairSchedulingParams params = cfMetaData.params.repairScheduling;

        Collection<ScheduledRepairTask> tasks = new ArrayList<>();

        for (Range<Token> range : ranges)
        {
            ScheduledRepairTask.Builder builder = new ScheduledRepairTask.Builder()
                    .withKeyspace(keyspace)
                    .withTable(table)
                    .withRange(range)
                    .withParams(params);

            Long repaired = repairHistory == null ? null : repairHistory.get(range);

            if (repaired != null)
                builder.withLastRepairedAt(repaired);

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
        Long downSince = downEndpoints.remove(endpoint);

        if (downSince != null)
        {
            long downTime = System.currentTimeMillis() - downSince;

            JobConfiguration configuration = new JobConfiguration.Builder()
                    .withMinimumDelay(3600)
                    .withPriority(BasePriority.HIGHEST)
                    .withEnabled(true)
                    .withRunOnce(true)
                    .build();

            List<ScheduledJob> jobs = new ArrayList<>();
            ScheduleableTableIterator it = new ScheduleableTableIterator();

            while (it.hasNext())
            {
                CFMetaData cfMetaData = it.next();

                if (downTime >= ScheduledRepairJob.getActualHintWindow(cfMetaData))
                {
                    String keyspace = cfMetaData.ksName;
                    String table = cfMetaData.cfName;

                    ScheduledRepairJob.Builder builder = new ScheduledRepairJob.Builder()
                            .withKeyspace(keyspace)
                            .withTable(table)
                            .withTasks(createRepairTasksForEndpoint(cfMetaData, endpoint))
                            .withRepairParams(cfMetaData.params.repairScheduling)
                            .withConfiguration(configuration);

                    jobs.add(builder.build());
                }
            }

            if (!jobs.isEmpty())
            {
                String uniqueId = "Repair-" + Integer.toString(Gossiper.instance.getCurrentGenerationNumber(endpoint));

                ScheduleManager.instance.schedule(RemoteScheduledJob.createJob(endpoint, uniqueId, jobs));
                ScheduleManager.instance.forceUpdate();
            }
        }
    }

    @Override
    public void onDown(InetAddress endpoint)
    {
        downEndpoints.put(endpoint, System.currentTimeMillis());
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
}
