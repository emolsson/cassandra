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
import java.util.*;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class DummyJob extends ScheduledJob
{
    public static final IVersionedSerializer<ScheduledJob> serializer = new DummyJobSerializer();

    private final Collection<DummyTask> tasks;
    private final String jobName;

    public DummyJob(JobConfiguration configuration, String jobName)
    {
        this(configuration, jobName, Arrays.asList(new DummyTask()));
    }

    private DummyJob(JobConfiguration configuration, String jobName, Collection<DummyTask> tasks)
    {
        super(configuration);

        this.jobName = jobName;
        this.tasks = tasks;
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
        return "DummyJob " + jobName;
    }

    @Override
    public IVersionedSerializer<ScheduledJob> getSerializer()
    {
        return serializer;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + getConfiguration().hashCode();
        result = prime * result + jobName.hashCode();
        result = prime * result + tasks.size();
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        DummyJob other = (DummyJob) obj;
        if (jobName == null)
        {
            if (other.tasks != null)
                return false;
        }
        else if (!jobName.equals(other.jobName))
        {
            return false;
        }
        if (tasks == null)
        {
            if (other.tasks != null)
                return false;
        }
        else if (other.tasks == null)
        {
            return false;
        }
        else if (tasks.size() != other.tasks.size() || !tasks.containsAll(other.tasks))
        {
            return false;
        }
        return true;
    }

    private static class DummyTask extends ScheduledTask
    {
        private final int id;

        public DummyTask()
        {
            this(new Random().nextInt());
        }

        private DummyTask(int id)
        {
            this.id = id;
        }

        @Override
        public void execute()
        {
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + id;
            return result;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            DummyTask other = (DummyTask) obj;
            if (id != other.id)
                return false;
            return true;
        }

        @Override
        public String toString()
        {
            return "DummyJob";
        }
    }

    private static class DummyJobSerializer implements IVersionedSerializer<ScheduledJob>
    {

        @Override
        public void serialize(ScheduledJob t, DataOutputPlus out, int version) throws IOException
        {
            assert t instanceof DummyJob;
            DummyJob job = (DummyJob) t;

            JobConfiguration.serializer.serialize(t.getConfiguration(), out, version);

            out.writeUTF(job.jobName);
            out.writeInt(job.tasks.size());
            for (DummyTask task : job.tasks)
            {
                out.writeInt(task.id);
            }
        }

        @Override
        public ScheduledJob deserialize(DataInputPlus in, int version) throws IOException
        {
            JobConfiguration configuration = JobConfiguration.serializer.deserialize(in, version);
            String jobName = in.readUTF();
            List<DummyTask> tasks = new ArrayList<>();

            int nTasks = in.readInt();
            for (int i = 0; i < nTasks; i++)
            {
                int id = in.readInt();
                tasks.add(new DummyTask(id));
            }

            return new DummyJob(configuration, jobName, tasks);
        }

        @Override
        public long serializedSize(ScheduledJob t, int version)
        {
            assert t instanceof DummyJob;
            DummyJob job = (DummyJob) t;

            long size = JobConfiguration.serializer.serializedSize(t.getConfiguration(), version);
            size += TypeSizes.sizeof(job.jobName);
            size += TypeSizes.sizeof(job.tasks.size());
            for (DummyTask task : job.tasks)
            {
                size += TypeSizes.sizeof(task.id);
            }

            return size;
        }

    }

}
