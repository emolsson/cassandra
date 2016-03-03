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
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A distributed lock factory using CAS to acquire and maintain locks.
 */
public class CasLockFactory implements IDistributedLockFactory
{
    private static final Logger logger = LoggerFactory.getLogger(CasLockFactory.class);

    public static CasLockFactory instance = new CasLockFactory();

    private static final int LOCK_TIME = 30;

    public static final String NAME = "system_lock";

    public static final String RESOURCE_LOCK = "resource_lock";

    public static final String LOCK_PRIORITY = "resource_lock_priority";

    protected static final CFMetaData Lock =
            compile(RESOURCE_LOCK,
                    "Resource lock",
                    "CREATE TABLE %s ("
                            + "resource text,"
                            + "host uuid,"
                            + "metadata map<text,text>,"
                            + "PRIMARY KEY (resource))");

    protected static final CFMetaData LockPriority =
            compile(LOCK_PRIORITY,
                    "Lock priority",
                    "CREATE TABLE %s ("
                            + "resource text,"
                            + "host uuid,"
                            + "priority int,"
                            + "PRIMARY KEY (resource, host))");

    public static KeyspaceMetadata metadata()
    {
        return KeyspaceMetadata.create(NAME, KeyspaceParams.simple(3), Tables.of(Lock, LockPriority));
    }

    private static CFMetaData compile(String name, String description, String schema)
    {
        return CFMetaData.compile(String.format(schema, name), NAME).defaultTimeToLive(LOCK_TIME).gcGraceSeconds(0).comment(description);
    }

    private CasLockFactory()
    {
    }

    @Override
    public DistributedLock tryLock(String resource, int priority, Map<String, String> metadata) throws LockException
    {
        assert resource != null && metadata != null;

        if (!sufficientNodesForLocking(resource))
        {
            logger.error("Not sufficient nodes to lock {}", resource);
            throw new LockException("Not sufficient nodes to lock");
        }

        try
        {
            logger.trace("Competing for resource {} with priority {}", resource, priority);
            if (compete(resource, priority))
            {
                logger.trace("Trying acquire lock for resource {}", resource);
                ByteBuffer serializedMetadata = serializeMap(metadata);
                if (lock(resource, serializedMetadata))
                {
                    logger.trace("Lock for resource {} acquired", resource);
                    ScheduledFuture<?> future = ScheduledExecutors.distributedScheduledTasks.scheduleWithFixedDelay(new LockUpdateTask(resource, serializedMetadata), 0, LOCK_TIME / 2, TimeUnit.SECONDS);

                    return new CASResourceLock(future);
                }
            }
        }
        catch (Throwable t)
        {
            logger.error("Unable to compete for/lock resource {}", resource, t);
            throw new LockException(t);
        }

        throw new LockException(String.format("Unable to lock resource %s", resource));
    }

    /**
     * Check if there is a sufficient number of live nodes to lock the resource.
     *
     * @param resource
     *            The resource to lock
     * @return True if there is a sufficient number of live nodes.
     */
    private boolean sufficientNodesForLocking(String resource)
    {
        ByteBuffer resourceKey = UTF8Type.instance.fromString(resource);
        List<InetAddress> naturalEndpoints = StorageService.instance.getNaturalEndpoints(NAME, resourceKey);

        return ConsistencyLevel.QUORUM.isSufficientLiveNodes(Keyspace.open(NAME), Iterables.filter(naturalEndpoints, isAlive));
    }

    /**
     * Compete for the resource using the specified priority.
     *
     * This will make the local priority available to other nodes to compete with.
     *
     * @param resource
     *            The resource to lock
     * @param priority
     *            The local priority to lock
     * @return True if this node has the highest priority to lock.
     */
    private boolean compete(String resource, int priority)
    {
        insertPriority(resource, priority);
        int highestPriority = getHighestPriorityForResource(resource);
        logger.trace("Highest priority for resource {}: {}", resource, highestPriority);
        return priority >= highestPriority;
    }

    /**
     * Insert the local priority of the resource in the resource priority table.
     *
     * @param resource
     *            The resource to lock
     * @param priority
     *            The local priority to lock
     */
    private void insertPriority(String resource, int priority)
    {
        String query = "INSERT INTO %s.%s (resource, host, priority) VALUES ('%s',%s,%d)";
        String fmtQuery = String.format(query, NAME, LOCK_PRIORITY,
                resource,
                StorageService.instance.getLocalHostUUID(),
                priority);

        processSilent(fmtQuery);
    }

    /**
     * Get the highest priority for the resource.
     *
     * @param resource
     *            The resource to lock
     * @return The highest priority to lock the resource
     */
    private int getHighestPriorityForResource(String resource)
    {
        String query = "SELECT priority FROM %s.%s WHERE resource='%s'";
        String fmtQuery = String.format(query, NAME, LOCK_PRIORITY,
                resource);

        UntypedResultSet res = processSilent(fmtQuery);

        if (res == null)
        {
            return Integer.MAX_VALUE;
        }

        return StreamSupport.stream(res.spliterator(), false)
                .map(row -> row.getInt("priority"))
                .max(Integer::compare)
                .orElse(0);
    }

    /**
     * Try to lock the resource using CAS.
     *
     * @param resource
     *            The resource to lock
     * @param metadata
     *            The metadata of the lock
     * @return True if the locking was successful
     */
    private boolean lock(String resource, ByteBuffer metadata)
    {
        String query = "INSERT INTO %s.%s (resource, host, metadata) VALUES ('%s', %s, ?) IF NOT EXISTS";
        String fmtQuery = String.format(query, NAME, RESOURCE_LOCK,
                resource,
                StorageService.instance.getLocalHostUUID());

        UntypedResultSet res = processSilent(fmtQuery, metadata);

        if (res == null)
            return false;

        return res.one().getBoolean("[applied]");
    }

    /**
     * Update the resource lock.
     *
     * This method will be called repeatedly while the local node has the lock.
     *
     * @param resource
     *            The resource to lock
     * @param metadata
     *            The metadata of the lock
     * @return True if able to update the resource lock
     */
    private boolean updateLock(String resource, ByteBuffer metadata)
    {
        UUID localHostUUID = StorageService.instance.getLocalHostUUID();
        String query = "UPDATE %s.%s SET host=%s, metadata=? WHERE resource='%s' IF host=%s";
        String fmtQuery = String.format(query, NAME, RESOURCE_LOCK,
                localHostUUID,
                resource,
                localHostUUID);

        UntypedResultSet res = processSilent(fmtQuery, metadata);

        if (res == null)
            return false;

        return res.one().getBoolean("[applied]");
    }

    private Predicate<InetAddress> isAlive = new Predicate<InetAddress>()
    {
        @Override
        public boolean apply(InetAddress endpoint)
        {
            return FailureDetector.instance.isAlive(endpoint);
        }
    };

    private class LockUpdateTask implements Runnable
    {
        private final String resource;
        private final ByteBuffer metadata;

        private LockUpdateTask(String resource, ByteBuffer metadata)
        {
            this.resource = resource;
            this.metadata = metadata;
        }

        @Override
        public void run()
        {
            try
            {
                int count = 0;
                while (count < 5 && !updateLock(resource, metadata))
                {
                    logger.trace("Unable to re-lock resource {}", resource);
                    Thread.sleep(1000);
                    count++;
                }
            }
            catch (Throwable t)
            {
                logger.error("Failed to re-lock resource {}", resource, t);
            }
        }

    }

    private class CASResourceLock implements DistributedLock
    {
        private final ScheduledFuture<?> future;

        private CASResourceLock(ScheduledFuture<?> future)
        {
            this.future = future;
        }

        @Override
        public void close() throws IOException
        {
            future.cancel(false);
        }

    }

    private static ByteBuffer serializeMap(Map<String, String> map)
    {
        return MapType.getInstance(UTF8Type.instance, UTF8Type.instance, true).getSerializer().serialize(map);
    }

    private static UntypedResultSet processSilent(String fmtQry, ByteBuffer... values)
    {
        try
        {
            return QueryProcessor.process(fmtQry, ConsistencyLevel.ONE, Lists.newArrayList(values));
        }
        catch (Throwable t)
        {
            logger.error("Error executing query " + fmtQry, t);
        }
        return null;
    }
}
