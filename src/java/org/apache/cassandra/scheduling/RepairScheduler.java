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
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.RepairSchedulingParams;
import org.apache.cassandra.service.MigrationListener;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The default repair scheduler used to create repair jobs.
 */
public class RepairScheduler extends MigrationListener implements IScheduler
{
    private static final Logger logger = LoggerFactory.getLogger(RepairScheduler.class);

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

        Collection<String> keyspaces = Schema.instance.getKeyspaces();

        for (String keyspaceName : keyspaces)
        {
            Keyspace keyspace = Keyspace.open(keyspaceName);

            if (keyspace.getReplicationStrategy().getReplicationFactor() < 2)
            {
                continue;
            }

            Collection<Range<Token>> ranges = StorageService.instance.getPrimaryRanges(keyspaceName);
            for (CFMetaData cfMetaData : keyspace.getMetadata().tables)
            {
                ScheduledJob job = maybeGetTableJob(cfMetaData, ranges);
                if (job != null)
                {
                    ret.add(job);
                    logger.info("Added scheduled repair job for {}.{}", keyspaceName, cfMetaData.cfName);
                }
            }
        }

        return ret;
    }

    private ScheduledJob maybeGetTableJob(CFMetaData cfMetaData, Collection<Range<Token>> ranges)
    {
        String keyspace = cfMetaData.ksName;
        String table = cfMetaData.cfName;

        String ksTb = keyspace + "." + table;
        if (cfMetaData.params.repairScheduling.enabled() && scheduledTables.add(ksTb))
        {
            try
            {
                Collection<ScheduledRepairTask> tasks = createTasks(keyspace, table, ranges,
                        cfMetaData.params.repairScheduling);

                // TODO: Read last run time of the tasks from the repair history

                return new ScheduledRepairJob(keyspace, table, tasks, cfMetaData.params.repairScheduling);
            }
            catch (ConfigurationException e)
            {
                scheduledTables.remove(ksTb);
                throw e;
            }
        }

        return null;
    }

    private Collection<ScheduledRepairTask> createTasks(String keyspace, String table, Collection<Range<Token>> ranges,
            RepairSchedulingParams params)
    {
        Collection<ScheduledRepairTask> ret = new ArrayList<>();

        for (Range<Token> range : ranges)
        {
            ret.add(new ScheduledRepairTask(keyspace, table, range, params));
        }

        return ret;
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

        for (Iterator<String> it = scheduledTables.iterator(); it.hasNext();)
        {
            String ksTb = it.next();

            if (ksTb.startsWith(ksStart))
                it.remove();
        }
    }

    @Override
    public void onDropColumnFamily(String ksName, String cfName)
    {
        ScheduleManager.instance.forceUpdate();

        scheduledTables.remove(ksName + "." + cfName);
    }

}
