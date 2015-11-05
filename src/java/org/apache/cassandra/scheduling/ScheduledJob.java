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
import java.lang.reflect.Field;
import java.util.Collection;

import org.apache.cassandra.cache.CachedValue;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.scheduling.DistributedLock.Lock;
import org.apache.cassandra.scheduling.DistributedLock.LockException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A job that gets created by an {@link IScheduler} and is then executed based on priority by the
 * {@link ScheduleManager}.
 *
 * <br>
 *
 * <br>
 *
 * The priority is calculated as:<br>
 * {@code P = (H+1) * bP}<br>
 * Where {@code P} is the priority, {@code H} is the number of hours since the job could have started and {@code bP} is
 * the base priority of the job.
 */
public abstract class ScheduledJob
{
    public static IVersionedSerializer<ScheduledJob> serializer = new ScheduledJobSerializer();

    private static final Logger logger = LoggerFactory.getLogger(ScheduledJob.class);

    private final CachedValue<Long> cachedRunTime = new CachedValue<>();

    private JobConfiguration configuration;

    protected ScheduledJob(JobConfiguration configuration)
    {
        assert configuration != null;

        this.configuration = configuration;
    }

    protected void init()
    {
        if (getLastRunTime() == -1)
        {
            long lastRunTime = System.currentTimeMillis() - this.configuration.getMinimumDelay();

            getTasks().forEach(p -> p.setLastRunTime(lastRunTime));
        }
    }

    protected abstract Collection<? extends ScheduledTask> getTasks();

    /**
     * Set the {@link JobConfiguration} for this job.
     *
     * @param configuration
     *            The new configuration.
     */
    protected final void setConfiguration(JobConfiguration configuration)
    {
        assert configuration != null;
        this.configuration = configuration;
    }

    /**
     * Get the current {@link JobConfiguration} for this job.
     *
     * @return
     */
    protected final JobConfiguration getConfiguration()
    {
        return this.configuration;
    }

    /**
     * Update the job to get new configuration, etc.
     *
     * @return False if the job should be removed.
     */
    public abstract boolean update();

    /**
     * Execute all tasks that this scheduled job contains.
     *
     * @return True on success.
     */
    public final boolean execute()
    {
        cachedRunTime.invalidate();

        getTasks().forEach(this::execute);

        return true;
    }

    private final void execute(ScheduledTask task)
    {
        try
        {
            if (task.getLastRunTime() + configuration.getMinimumDelay() < System.currentTimeMillis())
            {
                task.execute();
                task.setLastRunTime(System.currentTimeMillis());
            }
        }
        catch (Exception e)
        {
            logger.warn("Unable to run task", e);
            // TODO: Handle re-running of tasks later
        }
    }

    /**
     * @return True if the job should be run only once.
     */
    public boolean shouldRunOnce()
    {
        return configuration.runOnce();
    }

    /**
     * @return True if this job is enabled.
     */
    public boolean isEnabled()
    {
        return configuration.isEnabled();
    }

    /**
     * Calculates if this job could run now.
     *
     * @return True if the job can run now.
     */
    public final boolean couldRunNow()
    {
        return (getLastRunTime() + configuration.getMinimumDelay()) < System.currentTimeMillis();
    }

    /**
     * Calculates when the job could be run next.
     *
     * @return Time in milliseconds until next run.
     */
    public long timeToNextRun()
    {
        long nextRun = (getLastRunTime() + configuration.getMinimumDelay()) - System.currentTimeMillis();

        if (nextRun < 0)
            return 0;

        return nextRun;
    }

    /**
     * Calculates the priority of the job based on the time that has passed.
     *
     * @return
     */
    public final int getPriority()
    {
        int overridePrio = overridePriority();

        if (overridePrio > -1)
        {
            return overridePrio;
        }

        long now = System.currentTimeMillis();
        long diff = now - getLastRunTime();

        if (diff < 0)
            return -1;

        int hours = (int) (diff / (60 * 60 * 1000));

        // Using hours + 1 so that base priority works even if no time has passed
        return (hours + 1) * configuration.getBasePriority();
    }

    public int overridePriority()
    {
        return -1;
    }

    public Lock getLock() throws LockException
    {
        return DistributedLock.instance.tryGetLock(ScheduleManager.SCHEDULE_LOCK, getPriority());
    }

    private long getLastRunTime()
    {
        Long cached = cachedRunTime.getValue();
        if (cached != null)
            return cached;

        long ret = -1;

        for (ScheduledTask task : getTasks())
        {
            if (ret == -1 || task.getLastRunTime() < ret)
            {
                ret = task.getLastRunTime();
            }
        }

        cachedRunTime.set(ret);

        return ret;
    }

    public abstract String toString();

    /**
     * A serializer used to serialize the job when it should be sent to a remote host.
     *
     * @return
     */
    public abstract IVersionedSerializer<ScheduledJob> getSerializer();

    public static class ScheduledJobSerializer implements IVersionedSerializer<ScheduledJob>
    {

        @Override
        public void serialize(ScheduledJob t, DataOutputPlus out, int version) throws IOException
        {
            out.writeUTF(t.getClass().getCanonicalName());
            t.getSerializer().serialize(t, out, version);
        }

        @SuppressWarnings("unchecked")
        @Override
        public ScheduledJob deserialize(DataInputPlus in, int version) throws IOException
        {
            String className = in.readUTF();

            try
            {
                Class<?> clazz = Class.forName(className);
                assert clazz.isAssignableFrom(ScheduledJob.class);
                Field serializerField = clazz.getDeclaredField("serializer");
                Object serializer = serializerField.get(null);
                assert serializer instanceof IVersionedSerializer;

                return ((IVersionedSerializer<ScheduledJob>) serializer).deserialize(in, version);
            }
            catch (Exception e)
            {
                logger.error("Unable to get the scheduled job", e);
            }

            return null;
        }

        @Override
        public long serializedSize(ScheduledJob t, int version)
        {
            int size = TypeSizes.sizeof(t.getClass().getCanonicalName());
            size += t.getSerializer().serializedSize(t, version);

            return size;
        }

    }
}
