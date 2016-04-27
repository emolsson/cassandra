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

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.LeaseException;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.service.StorageService;

/**
 * A distributed lease factory using CAS to acquire and maintain leases.
 */
public class CasLeaseFactory implements LeaseFactory
{
    public static final int DEFAULT_LEASE_TIME = 60;
    public static final String KEYSPACE_NAME = "system_leases";
    public static final String RESOURCE_LEASE = "resource_lease";
    public static final String RESOURCE_LEASE_PRIORITY = "resource_lease_priority";
    protected static final CFMetaData lease =
            compile(RESOURCE_LEASE,
                    "Resource lease",
                    "CREATE TABLE %s ("
                            + "resource text,"
                            + "host uuid,"
                            + "metadata map<text,text>,"
                            + "PRIMARY KEY (resource))");
    protected static final CFMetaData leaseFactory =
            compile(RESOURCE_LEASE_PRIORITY,
                    "Lease priority",
                    "CREATE TABLE %s ("
                            + "resource text,"
                            + "host uuid,"
                            + "priority int,"
                            + "isActive boolean,"
                            + "PRIMARY KEY (resource, host))");
    private static final Logger logger = LoggerFactory.getLogger(CasLeaseFactory.class);
    public static CasLeaseFactory instance = new CasLeaseFactory();

    private CasLeaseFactory()
    {
    }

    public static KeyspaceMetadata metadata()
    {
        return KeyspaceMetadata.create(KEYSPACE_NAME, KeyspaceParams.simple(3), Tables.of(lease, leaseFactory));
    }

    private static CFMetaData compile(String name, String description, String schema)
    {
        return CFMetaData.compile(String.format(schema, name), KEYSPACE_NAME).defaultTimeToLive(DEFAULT_LEASE_TIME)
                       .gcGraceSeconds(0).comment(description);
    }

    private static ByteBuffer serializeMap(Map<String, String> map)
    {
        return MapType.getInstance(UTF8Type.instance, UTF8Type.instance, true).getSerializer().serialize(map);
    }

    private static UntypedResultSet processSilent(String fmtQry, ByteBuffer... values)
    {
        return processSilent(fmtQry, ConsistencyLevel.QUORUM, values);
    }

    private static UntypedResultSet processSilent(String fmtQry, ConsistencyLevel cl, ByteBuffer... values)
    {
        try
        {
            return QueryProcessor.process(fmtQry, cl, Lists.newArrayList(values));
        }
        catch (Throwable t)
        {
            logger.error("Error executing query " + fmtQry, t);
        }
        return null;
    }

    @Override
    public Optional<Lease> newLease(String resource, int priority, Map<String, String> metadata) throws LeaseException
    {
        assert resource != null && metadata != null;

        try
        {
            logger.trace("Competing for resource {} with priority {}", resource, priority);
            if (compete(resource, priority))
            {
                logger.trace("Trying acquire lease for resource {}", resource);
                ByteBuffer serializedMetadata = serializeMap(metadata);
                if (lease(resource, serializedMetadata))
                {
                    logger.trace("Lease for resource {} acquired", resource);
                    return Optional.of(new CASResourceLease(resource, serializedMetadata, DEFAULT_LEASE_TIME));
                }
            }
        }
        catch (Throwable t)
        {
            logger.error("Unable to compete for/lease resource {}", resource, t);
            throw new LeaseException(t);
        }

        return Optional.empty();
    }

    /**
     * Compete for the resource using the specified priority.
     * <p>
     * This will make the local priority available to other nodes to compete with.
     *
     * @param resource The resource to lease
     * @param priority The local priority to lease
     * @return True if this node has the highest priority to lease
     */
    private boolean compete(String resource, int priority)
    {
        insertPriority(resource, priority);
        int highestPriority = getHighestPriorityForResource(resource);
        logger.trace("Highest priority for resource {}: {}", resource, highestPriority);
        return priority >= highestPriority;
    }

    /**
     * Insert the local priority of the resource in the resource priority table as an inactive priority.
     *
     * @param resource The resource to lease
     * @param priority The local priority to lease the resource
     */
    private void insertPriority(String resource, int priority)
    {
        insertPriority(resource, priority, false, DEFAULT_LEASE_TIME);
    }

    /**
     * Insert the local priority of the resource in the resource priority table.
     *
     * @param resource The resource to lease
     * @param priority The local priority to lease the resource
     * @param active   If true, other nodes won't try to lease the resource.
     * @param duration The duration the priority should be left in the database if not updated again
     */
    private void insertPriority(String resource, int priority, boolean active, int duration)
    {
        String query = "INSERT INTO %s.%s (resource, host, priority, isActive) VALUES ('%s',%s,%d, %s) USING TTL %d";
        String fmtQuery = String.format(query, KEYSPACE_NAME, RESOURCE_LEASE_PRIORITY,
                resource,
                StorageService.instance.getLocalHostUUID(),
                priority,
                active,
                duration);

        processSilent(fmtQuery);
    }

    /**
     * Clear the local priority from the resource in the resource priority table.
     *
     * @param resource The resource to remove the priority from
     */
    private void clearPriority(String resource)
    {
        String query = "DELETE FROM %s.%s WHERE resource = '%s' AND host=%s";
        String fmtQuery = String.format(query, KEYSPACE_NAME, RESOURCE_LEASE_PRIORITY,
                resource,
                StorageService.instance.getLocalHostUUID());

        processSilent(fmtQuery);
    }

    /**
     * Get the highest priority for the resource.
     *
     * @param resource The resource to lease
     * @return The highest priority to lease the resource
     */
    private int getHighestPriorityForResource(String resource)
    {
        String query = "SELECT isActive, priority FROM %s.%s WHERE resource='%s'";
        String fmtQuery = String.format(query, KEYSPACE_NAME, RESOURCE_LEASE_PRIORITY,
                resource);

        UntypedResultSet res = processSilent(fmtQuery);

        if (res == null)
        {
            return Integer.MAX_VALUE;
        }

        return StreamSupport.stream(res.spliterator(), false)
                       .map(row -> row.has("isactive") && row.getBoolean("isactive") ?
                                           Integer.MAX_VALUE :
                                           row.getInt("priority"))
                       .max(Integer::compare)
                       .orElse(0);
    }

    /**
     * Try to lease the resource using CAS.
     *
     * @param resource The resource to lease
     * @param metadata The metadata of the lease
     * @return True if the leasing was successful
     */
    private boolean lease(String resource, ByteBuffer metadata)
    {
        String query = "INSERT INTO %s.%s (resource, host, metadata) VALUES ('%s', %s, ?) IF NOT EXISTS USING TTL %d";
        String fmtQuery = String.format(query, KEYSPACE_NAME, RESOURCE_LEASE,
                resource,
                StorageService.instance.getLocalHostUUID(),
                DEFAULT_LEASE_TIME);

        UntypedResultSet res = processSilent(fmtQuery, metadata);

        if (res != null && res.one().getBoolean("[applied]"))
        {
            insertPriority(resource, -1, true, DEFAULT_LEASE_TIME);
            return true;
        }

        return false;
    }

    /**
     * Update the resource lease.
     * <p>
     * This method will be called repeatedly while the local node has the lease.
     *
     * @param resource The resource to lease
     * @param metadata The metadata of the lease
     * @return True if able to update the resource lease
     */
    private boolean updateLease(String resource, ByteBuffer metadata, int duration)
    {
        UUID localHostUUID = StorageService.instance.getLocalHostUUID();
        String query = "UPDATE %s.%s USING TTL %d SET host=%s, metadata=? WHERE resource='%s' IF host=%s";
        String fmtQuery = String.format(query, KEYSPACE_NAME, RESOURCE_LEASE,
                duration,
                localHostUUID,
                resource,
                localHostUUID);

        UntypedResultSet res = processSilent(fmtQuery, metadata);

        if (res != null && res.one().getBoolean("[applied]"))
        {
            insertPriority(resource, -1, true, duration);
            return true;
        }

        return false;
    }

    /**
     * Check if the resource lease is held by the local node.
     *
     * @param resource The resource
     * @return True if the lease is held by the local node
     */
    private boolean holdsLease(String resource)
    {
        UUID localHostUUID = StorageService.instance.getLocalHostUUID();
        String query = "SELECT host FROM %s.%s where resource='%s'";
        String fmtQuery = String.format(query, KEYSPACE_NAME, RESOURCE_LEASE,
                resource);

        UntypedResultSet res = processSilent(fmtQuery, ConsistencyLevel.SERIAL);

        return res != null && localHostUUID.equals(res.one().getUUID("host"));
    }

    private boolean clearLease(String resource)
    {
        UUID localHostUUID = StorageService.instance.getLocalHostUUID();
        String query = "DELETE FROM %s.%s WHERE resource='%s' IF host=%s";
        String fmtQuery = String.format(query, KEYSPACE_NAME, RESOURCE_LEASE,
                resource,
                localHostUUID);

        UntypedResultSet res = processSilent(fmtQuery);

        if (res != null && res.one().getBoolean("[applied]"))
        {
            clearPriority(resource);
            return true;
        }

        return false;
    }

    private class CASResourceLease implements Lease
    {
        private final String resource;
        private final ByteBuffer metadata;
        private volatile long expirationTime;

        private CASResourceLease(String resource, ByteBuffer metadata, int duration)
        {
            this.resource = resource;
            this.metadata = metadata;
            expirationTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(duration);
        }

        @Override
        public long getExpiration()
        {
            return expirationTime;
        }

        @Override
        public boolean renew(int duration) throws LeaseException
        {
            if (hasExpired())
                return false;

            try
            {
                if (updateLease(resource, metadata, duration))
                {
                    expirationTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(duration);
                    return true;
                }
                return false;
            }
            catch (Throwable t)
            {
                logger.error("Unable to renew lease for {}", resource, t);
                throw new LeaseException(t);
            }
        }

        @Override
        public boolean cancel() throws LeaseException
        {
            if (hasExpired())
                return false;

            try
            {
                expirationTime = -1;
                return clearLease(resource);
            }
            catch (Throwable t)
            {
                logger.error("Unable to cancel lease for {}", resource, t);
                throw new LeaseException(t);
            }
        }

        @Override
        public boolean isValid()
        {
            return !hasExpired() && holdsLease(resource);
        }

        private boolean hasExpired()
        {
            return getExpiration() < System.currentTimeMillis();
        }
    }
}
