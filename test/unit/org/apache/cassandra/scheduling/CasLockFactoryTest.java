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
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.HashMap;

import com.google.common.collect.Lists;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.scheduling.IDistributedLockFactory.DistributedLock;
import org.apache.cassandra.scheduling.IDistributedLockFactory.LockException;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import junit.framework.*;

public class CasLockFactoryTest
{
    @BeforeClass
    public static void defineSchema() throws ConfigurationException, UnknownHostException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(CasLockFactory.NAME, KeyspaceParams.simple(1),
                CasLockFactory.LockPriority,
                CasLockFactory.Lock);

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

    @Test
    public void testLock() throws LockException, IOException
    {
        try (DistributedLock lock = CasLockFactory.instance.tryLock("lock1", 1, new HashMap<>()))
        {
        }
    }

    @Test
    public void testSameLockTwice() throws LockException, IOException
    {
        try (DistributedLock lock = CasLockFactory.instance.tryLock("lock2", 1, new HashMap<>()))
        {
            try (DistributedLock lock2 = CasLockFactory.instance.tryLock("lock2", 1, new HashMap<>()))
            {
                Assert.assertTrue(false);
            }
            catch (LockException e)
            {
            }
        }
    }

    @Test
    public void testLockWithLowerPriority() throws IOException
    {
        insertPriority("lock3", 2);

        try (DistributedLock lock = CasLockFactory.instance.tryLock("lock3", 1, new HashMap<>()))
        {
            Assert.assertTrue(false);
        }
        catch (LockException e)
        {
        }
    }

    public void insertPriority(String resource, int priority)
    {
        String query = "INSERT INTO %s.%s (resource, host, priority) VALUES ('%s',%s,%d)";
        String fmtQuery = String.format(query, CasLockFactory.NAME, CasLockFactory.LOCK_PRIORITY,
                resource,
                UUIDGen.getTimeUUID(),
                priority);

        process(fmtQuery);
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
}
