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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.net.*;
import org.apache.cassandra.scheduling.JobConfiguration.BasePriority;
import org.apache.cassandra.utils.concurrent.SimpleCondition;
import org.apache.cassandra.io.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RemoteScheduledJobTest
{

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        SchemaLoader.prepareServer();
    }

    @After
    public void tearDown()
    {
        MessagingService.instance().clearMessageSinks();
    }

    @Test
    public void testValidatorComplete() throws Throwable
    {
        JobConfiguration configuration = new JobConfiguration.Builder().withMinimumDelay(1000)
                .withPriority(BasePriority.LOW).withEnabled(true).build();

        final DummyJob job = new DummyJob(configuration, "TestDummyJob");

        final IVerbHandler<ScheduledJob> handler = new IVerbHandler<ScheduledJob>()
        {

            @Override
            public void doVerb(MessageIn<ScheduledJob> message, int id) throws IOException
            {
                assertTrue(message.payload instanceof DummyJob);
                assertEquals(job, message.payload);
            }

        };

        final SimpleCondition lock = new SimpleCondition();
        MessagingService.instance().addMessageSink(new IMessageSink()
        {
            public boolean allowOutgoingMessage(MessageOut message, int id, InetAddress to)
            {
                try
                {
                    if (message.verb == MessagingService.Verb.SCHEDULED_JOB)
                    {
                        assertTrue(message.payload instanceof DummyJob);
                        assertEquals(job, message.payload);

                        ByteArrayOutputStream stream = new ByteArrayOutputStream();

                        DataOutputPlus out = new WrappedDataOutputStreamPlus(stream);

                        message.serialize(out, -1);

                        DataInputPlus in = new DataInputPlus.DataInputStreamPlus(new ByteArrayInputStream(stream
                                .toByteArray()));

                        MessageIn<ScheduledJob> messageIn = MessageIn.read(in, -1, id);

                        assertTrue(messageIn.payload instanceof DummyJob);
                        assertEquals(job, messageIn.payload);
                    }
                }
                catch (IOException e)
                {
                    assertTrue(false);
                }
                finally
                {
                    lock.signalAll();
                }
                return false;
            }

            public boolean allowIncomingMessage(MessageIn message, int id)
            {
                return false;
            }
        });

        InetAddress remote = InetAddress.getByName("127.0.0.1");

        ScheduledJob remoteJob = RemoteScheduledJob.createJob(remote, Arrays.asList(job));

        for (ScheduledTask task : remoteJob.getTasks())
        {
            assertTrue(task.execute());
        }

        if (!lock.isSignaled())
            lock.await();
    }
}
