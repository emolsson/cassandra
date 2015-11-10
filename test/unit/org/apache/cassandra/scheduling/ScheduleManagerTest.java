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
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.scheduling.DistributedLock.Lock;
import org.apache.cassandra.scheduling.DistributedLock.LockException;
import org.apache.cassandra.scheduling.JobConfiguration.BasePriority;
import org.apache.cassandra.utils.concurrent.SimpleCondition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class ScheduleManagerTest
{
    private ScheduleManager scheduleManager;

    @Before
    public void startup()
    {
        scheduleManager = ScheduleManager.getManagerForTest();
        scheduleManager.startup(new DummyPolicy());
    }

    @After
    public void shutdown()
    {
        scheduleManager.shutdown();
    }

    @Test
    public void testScheduleJobNow() throws InterruptedException
    {
        JobConfiguration configuration = getJobConfiguration();

        final long lastRunTime = System.currentTimeMillis() - 1000;
        final SimpleCondition lock = new SimpleCondition();

        scheduleJob(configuration, lastRunTime, lock);

        assertTrue(lock.await(2, TimeUnit.SECONDS));
    }

    @Test
    public void testScheduleJobNowWithThrowingLock() throws InterruptedException
    {
        JobConfiguration configuration = getJobConfiguration();

        final long lastRunTime = System.currentTimeMillis() - 1000;
        final SimpleCondition lock = new SimpleCondition();

        scheduleManager.schedule(new DummyJob(configuration, new ScheduledTask()
        {
            @Override
            public long getLastRunTime()
            {
                return lastRunTime;
            }

            @Override
            public void execute()
            {
                lock.signalAll();
            }

            @Override
            public String toString()
            {
                return "DummyTask";
            }
        }, null));

        assertFalse(lock.await(2, TimeUnit.SECONDS));
    }

    @Test
    public void testScheduleJob1Second() throws InterruptedException
    {
        JobConfiguration configuration = getJobConfiguration();

        final long lastRunTime = System.currentTimeMillis();
        final SimpleCondition lock = new SimpleCondition();

        scheduleJob(configuration, lastRunTime, lock);

        Thread.sleep(500);
        assertFalse(lock.isSignaled());
        assertTrue(lock.await(3, TimeUnit.SECONDS));
    }

    @Test
    public void testSchedule2JobsRunOrder() throws InterruptedException
    {
        JobConfiguration configuration = getJobConfiguration();
        JobConfiguration configuration2 = getJobConfiguration(BasePriority.HIGH);

        final long lastRunTime = System.currentTimeMillis() - 1000;
        final SimpleCondition lock = new SimpleCondition();
        final SimpleCondition lock2 = new SimpleCondition();

        scheduleJob(configuration, lastRunTime, lock2);
        scheduleJob(configuration2, lastRunTime, lock);

        Thread.sleep(500);
        assertFalse(lock.isSignaled());
        assertTrue(lock.await(2, TimeUnit.SECONDS));
        assertFalse(lock2.isSignaled());
        assertTrue(lock2.await(2, TimeUnit.SECONDS));
    }

    @Test
    public void testDummyPolicy() throws InterruptedException
    {
        JobConfiguration configuration = getJobConfiguration();

        final long lastRunTime = System.currentTimeMillis() - 1000;
        final SimpleCondition lock = new SimpleCondition();

        scheduleManager.startup(new ISchedulePolicy()
        {
            boolean triggered = false;

            @Override
            public long validate(ScheduledJob job)
            {
                if (!triggered)
                {
                    triggered = true;
                    return 1000;
                }

                return -1;
            }
        });

        scheduleJob(configuration, lastRunTime, lock);

        Thread.sleep(1000);
        assertFalse(lock.isSignaled());
        assertTrue(lock.await(2, TimeUnit.SECONDS));
    }

    private JobConfiguration getJobConfiguration()
    {
        return getJobConfiguration(BasePriority.LOW);
    }

    private JobConfiguration getJobConfiguration(BasePriority priority)
    {
        return new JobConfiguration.Builder().withEnabled(true).withRunOnce(true).withMinimumDelay(1)
                .withPriority(priority).build();
    }

    private void scheduleJob(final JobConfiguration configuration, final long lastRunTime, final SimpleCondition lock)
    {
        scheduleManager.schedule(new DummyJob(configuration, new ScheduledTask()
        {
            @Override
            public long getLastRunTime()
            {
                return lastRunTime;
            }

            @Override
            public void execute()
            {
                lock.signalAll();
            }

            @Override
            public String toString()
            {
                return "DummyTask";
            }
        }));
    }

    private class DummyPolicy implements ISchedulePolicy
    {

        @Override
        public long validate(ScheduledJob job)
        {
            return -1;
        }
    }

    private class DummyJob extends ScheduledJob
    {

        private final Collection<ScheduledTask> tasks;
        private final DistributedLock.Lock lock;

        /**
         * @param configuration
         */
        protected DummyJob(JobConfiguration configuration, ScheduledTask task)
        {
            this(configuration, task, new DistributedLock.Lock()
            {
                @Override
                public void close() throws IOException
                {
                    // Dummy
                }
            });
        }

        protected DummyJob(JobConfiguration configuration, ScheduledTask task, DistributedLock.Lock lock)
        {
            super(configuration);
            tasks = Arrays.asList(task);
            this.lock = lock;
        }

        @Override
        public Lock getLock() throws LockException
        {
            if (lock == null)
            {
                throw new LockException("Dummy");
            }
            return lock;
        }

        @Override
        protected Collection<? extends ScheduledTask> getTasks()
        {
            return tasks;
        }

        @Override
        public boolean update()
        {
            return true;
        }

        @Override
        public String toString()
        {
            return "DummyTask";
        }

        @Override
        public IVersionedSerializer<ScheduledJob> getSerializer()
        {
            // TODO Auto-generated method stub
            return null;
        }

    }
}
