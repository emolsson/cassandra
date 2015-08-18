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

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.UntypedResultSet.Row;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class used to create distributed locks for resources.
 *
 * <br>
 *
 * <br>
 *
 * Uses priority to avoid starvation.
 */
public class DistributedLock
{
    private static final Logger logger = LoggerFactory.getLogger(DistributedLock.class);

    private static final int LOCK_TIME = 30;

    public static final DistributedLock instance = new DistributedLock();

    public static final String NAME = "system_distributed";
    public static final String LOCK = "lock";
    public static final String LOCK_PRIORITY = "lock_priority";

    private static final CFMetaData Lock =
            compile(LOCK,
                    "Lock",
                    "CREATE TABLE %s ("
                  + "resource text,"
                  + "PRIMARY KEY (resource))");

    private static final CFMetaData LockPriority =
            compile(LOCK_PRIORITY,
                    "Lock priority",
                    "CREATE TABLE %s ("
                  + "resource text,"
                  + "node uuid,"
                  + "priority int,"
                  + "PRIMARY KEY (resource, node))");

    private static CFMetaData compile(String name, String description, String schema)
    {
        return CFMetaData.compile(String.format(schema, name), NAME).gcGraceSeconds(0).defaultTimeToLive(LOCK_TIME)
                .comment(description);
    }

    public static List<CFMetaData> metadata()
    {
        return Arrays.asList(Lock, LockPriority);
    }

    /**
     * @return The lock time in milliseconds.
     */
    public static long getLockTime()
    {
        return LOCK_TIME * 1000;
    }

    private DistributedLock()
    {
    }

    /**
     * Try to get a lock on a resource that will hold the lock until the lock object is closed.
     *
     * <br>
     *
     * <br>
     *
     * Uses the priority to check if any other node has precedence over this.
     *
     * @param resource
     *            The resource to lock.
     * @param priority
     *            The priority of the lock.
     * @return A lock object that when closed will stop the lock from re-locking.
     * @throws LockException
     *             Thrown when unable to get the lock.
     */
    public Lock tryGetLock(String resource, int priority) throws LockException
    {
        if (!sufficientNodesForLocking(resource))
        {
            logger.debug("Not sufficient nodes to lock");
            return new NullLock();
        }

        try
        {
            writePriority(resource, priority);

            if (shouldTryLock(resource, priority) && tryLock(resource))
            {
                ScheduledFuture<?> future = ScheduledExecutors.scheduledLongTasks.scheduleAtFixedRate(
                        new LockUpdateTask(resource), LOCK_TIME / 2, LOCK_TIME / 2, TimeUnit.SECONDS);

                return new CASLock(future);
            }
        }
        catch (Exception e)
        {
            throw new LockException("Unable to lock", e);
        }

        throw new LockException(String.format("Unable to lock resource '%s'", resource));
    }

    /**
     * Write the local nodes priority for the resource so that other nodes can read it.
     *
     * @param resource
     * @param priority
     */
    private void writePriority(String resource, int priority)
    {
        String query = "INSERT INTO %s.%s (resource, node, priority) VALUES ('%s', %s, %d)";
        String fmtQuery = String.format(query, NAME, LOCK_PRIORITY, resource, StorageService.instance.getLocalHostId(),
                priority);

        process(fmtQuery);
    }

    /**
     * Used to check if any other node has higher priority than this one before trying to take the lock.
     *
     * @param resource
     * @param priority
     * @return True if no other node has higher priority.
     */
    private boolean shouldTryLock(String resource, int priority)
    {
        String query = "SELECT priority FROM %s.%s WHERE resource='%s'";
        String fmtQuery = String.format(query, NAME, LOCK_PRIORITY, resource);

        UntypedResultSet result = process(fmtQuery);

        for (Row row : result)
        {
            if (row.getInt("priority") > priority)
                return false;
        }

        return true;
    }

    /**
     * Try to take the lock for the resource.
     *
     * @param resource
     *            The resource to lock.
     * @return True if able to lock the resource.
     */
    private boolean tryLock(String resource)
    {
        String query = "INSERT INTO %s.%s (resource) VALUES ('%s') IF NOT EXISTS";
        String fmtQuery = String.format(query, NAME, LOCK, resource);

        UntypedResultSet result = QueryProcessor.process(fmtQuery, ConsistencyLevel.ONE, ConsistencyLevel.LOCAL_SERIAL);

        for (Row row : result)
        {
            return row.getBoolean("[applied]");
        }

        return false;
    }

    private UntypedResultSet process(String fmtQry)
    {
        return QueryProcessor.process(fmtQry, ConsistencyLevel.ONE);
    }

    private boolean sufficientNodesForLocking(String resource)
    {
        List<InetAddress> nodes = StorageService.instance.getNaturalEndpoints(NAME, ByteBufferUtil.bytes(resource));

        return nodes.size() > 1;
    }

    /**
     * A locked resource that gets released by the call of the {@link Lock#close() close()} method.
     */
    public static interface Lock extends Closeable
    {

        /**
         * Releases the locked resource.
         */
        public void close() throws IOException;
    }

    /**
     * Dummy implementation for single node.
     */
    private class NullLock implements Lock
    {
        @Override
        public void close() throws IOException
        {
            // NOOP
        }
    }

    /**
     * Lock which is held by CAS that cancels the lock update task when closed.
     */
    private class CASLock implements Lock
    {
        private final ScheduledFuture<?> future;

        private CASLock(ScheduledFuture<?> future)
        {
            this.future = future;
        }

        @Override
        public void close() throws IOException
        {
            future.cancel(true);
        }
    }

    /**
     * Task that is used to update the lock so that the TTL doesn't expire.
     */
    private class LockUpdateTask implements Runnable
    {
        private final String resource;
        private final String fmtQuery;

        private LockUpdateTask(String resource)
        {
            this.resource = resource;

            String query = "INSERT INTO %s.%s (resource) VALUES ('%s')";
            fmtQuery = String.format(query, NAME, LOCK, resource);
        }

        @Override
        public void run()
        {
            boolean bDone = false;
            while (!bDone)
            {
                try
                {
                    process(fmtQuery);
                    bDone = true;
                }
                catch (Exception e)
                {
                    logger.warn(String.format("Unable to relock resource '%s'", resource), e);
                }
            }
        }
    }

    /**
     * Exception used to indicate a problem when trying to lock a resource.
     */
    public static class LockException extends Exception
    {
        private static final long serialVersionUID = 8873512893462752121L;

        public LockException(String message)
        {
            super(message);
        }

        public LockException(String message, Throwable t)
        {
            super(message, t);
        }

        public LockException(Throwable t)
        {
            super(t);
        }
    }
}
