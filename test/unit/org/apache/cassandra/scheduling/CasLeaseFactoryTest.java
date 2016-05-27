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
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.LeaseException;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.UUIDGen;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class CasLeaseFactoryTest
{
    @BeforeClass
    public static void defineSchema() throws ConfigurationException, UnknownHostException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(CasLeaseFactory.KEYSPACE_NAME, KeyspaceParams.simple(1),
                CasLeaseFactory.leaseFactory,
                CasLeaseFactory.lease);

        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        InetAddress localhost = InetAddress.getByName("127.0.0.1");
        metadata.updateNormalToken(Util.token("A"), localhost);
        metadata.updateHostId(UUIDGen.getTimeUUID(), localhost);
        MessagingService.instance().listen();
    }

    @AfterClass
    public static void cleanup()
    {
        MessagingService.instance().shutdown();
        StorageService.instance.getTokenMetadata().clearUnsafe();
    }

    private static UntypedResultSet process(String fmtQry, ByteBuffer... values)
    {
        try
        {
            return QueryProcessor.process(fmtQry, ConsistencyLevel.ONE, Lists.newArrayList(values));
        }
        catch (Throwable t)
        {
            throw new RuntimeException(t);
        }
    }

    @After
    public void testCleanup()
    {
        clearPriority("lease");
        clearLease("lease");
    }

    @Test
    public void testLease() throws LeaseException
    {
        Lease lease = CasLeaseFactory.instance.newLease("lease", 1, new HashMap<>()).orElse(null);
        assertNotNull(lease);

        assertTrue(lease.cancel());
    }

    @Test
    public void testSameLeaseTwice() throws LeaseException
    {
        Lease lease = CasLeaseFactory.instance.newLease("lease", 1, new HashMap<>()).orElse(null);
        assertNotNull(lease);
        assertFalse(CasLeaseFactory.instance.newLease("lease", 1, new HashMap<>()).isPresent());

        assertTrue(lease.cancel());
    }

    @Test
    public void testLeaseAgainstHigherInactivePriority() throws LeaseException
    {
        insertPriority("lease", 2, false);

        Lease lease = CasLeaseFactory.instance.newLease("lease", 1, new HashMap<>()).orElse(null);
        assertNull(lease);
    }

    @Test
    public void testLeaseAgainstLowerActivePriority() throws LeaseException
    {
        insertPriority("lease", 1, true);

        Lease lease = CasLeaseFactory.instance.newLease("lease", 2, new HashMap<>()).orElse(null);
        assertNull(lease);
    }

    @Test
    public void testLeaseAgain() throws LeaseException
    {
        Lease lease = CasLeaseFactory.instance.newLease("lease", 1, new HashMap<>()).orElse(null);
        assertNotNull(lease);

        assertTrue(lease.cancel());

        lease = CasLeaseFactory.instance.newLease("lease", 1, new HashMap<>()).orElse(null);
        assertNotNull(lease);

        assertTrue(lease.cancel());
    }

    @Test
    public void testRenewLease() throws LeaseException
    {
        long start = System.currentTimeMillis();
        Lease lease = CasLeaseFactory.instance.newLease("lease", 1, new HashMap<>()).orElse(null);

        assertNotNull(lease);

        assertTrue(lease.getExpiration() >= start + TimeUnit.SECONDS.toMillis(CasLeaseFactory.DEFAULT_LEASE_TIME));
        start = System.currentTimeMillis();
        assertTrue(lease.renew(10));
        assertTrue(lease.getExpiration() >= start + TimeUnit.SECONDS.toMillis(10));

        assertTrue(lease.cancel());
    }

    @Test
    public void testLeaseIsValid() throws LeaseException
    {
        Lease lease = CasLeaseFactory.instance.newLease("lease", 1, new HashMap<>()).orElse(null);
        assertNotNull(lease);
        assertTrue(lease.isValid());

        assertTrue(lease.cancel());
    }

    @Test
    public void testLeaseIsNotValid() throws LeaseException, InterruptedException
    {
        Lease lease = CasLeaseFactory.instance.newLease("lease", 1, new HashMap<>()).orElse(null);
        assertNotNull(lease);

        assertTrue(lease.renew(1));
        Thread.sleep(1500);
        assertFalse(lease.isValid());

        assertFalse(lease.cancel());
    }

    @Test
    public void testRemovedLeaseIsNotValid() throws LeaseException
    {
        Lease lease = CasLeaseFactory.instance.newLease("lease", 1, new HashMap<>()).orElse(null);
        assertNotNull(lease);

        clearLease("lease");

        assertFalse(lease.isValid());
    }

    @Test
    public void testRemovedLeaseIsNotRenewable() throws LeaseException
    {
        Lease lease = CasLeaseFactory.instance.newLease("lease", 1, new HashMap<>()).orElse(null);
        assertNotNull(lease);

        clearLease("lease");

        assertFalse(lease.renew(10));

        clearPriority("lease");
    }

    @Test
    public void testRemovedLeasePriorityIsNotLeasable() throws LeaseException
    {
        Lease lease = CasLeaseFactory.instance.newLease("lease", 1, new HashMap<>()).orElse(null);
        assertNotNull(lease);

        clearPriority("lease");

        assertFalse(CasLeaseFactory.instance.newLease("lease", 1, new HashMap<>()).isPresent());

        lease.cancel();
    }

    @Test
    public void testReacquireLeaseAfterShorterRenewalTime() throws LeaseException
    {
        Lease lease = CasLeaseFactory.instance.newLease("lease", 1, new HashMap<>()).orElse(null);
        assertNotNull(lease);

        assertTrue(lease.renew(10));
        assertTrue(lease.cancel());

        lease = CasLeaseFactory.instance.newLease("lease", 1, new HashMap<>()).orElse(null);
        assertNotNull(lease);

        assertTrue(lease.cancel());
    }

    private void insertPriority(String resource, int priority, boolean active)
    {
        String query = "INSERT INTO %s.%s (resource, host, priority, isActive) VALUES ('%s',%s,%d, %s)";
        String fmtQuery = String.format(query, CasLeaseFactory.KEYSPACE_NAME, CasLeaseFactory.RESOURCE_LEASE_PRIORITY,
                resource,
                UUIDGen.getTimeUUID(),
                priority,
                active);

        process(fmtQuery);
    }

    private void clearPriority(String resource)
    {
        String query = "DELETE FROM %s.%s WHERE resource='%s'";
        String fmtQuery = String.format(query, CasLeaseFactory.KEYSPACE_NAME, CasLeaseFactory.RESOURCE_LEASE_PRIORITY,
                resource);

        process(fmtQuery);
    }

    private void clearLease(String resource)
    {
        String query = "DELETE FROM %s.%s WHERE resource='%s' IF host = %s";
        String fmtQuery = String.format(query, CasLeaseFactory.KEYSPACE_NAME, CasLeaseFactory.RESOURCE_LEASE,
                resource,
                StorageService.instance.getLocalHostUUID());

        process(fmtQuery);
    }
}
