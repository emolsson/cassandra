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

import org.apache.cassandra.exceptions.ScheduledExecutionException;

/**
 * A scheduled task that gets executed by a {@link ScheduledJob}.
 */
public abstract class ScheduledTask
{
    private volatile long lastRunTime;

    public ScheduledTask()
    {
        this(-1);
    }

    public ScheduledTask(long lastRunTime)
    {
        this.lastRunTime = lastRunTime;
    }

    /**
     * @return The last time this task was run in milliseconds.
     */
    public long getLastRunTime()
    {
        return lastRunTime;
    }

    public void setLastRunTime(long time)
    {
        lastRunTime = time;
    }

    /**
     * Execute the task.
     *
     * @return True on success.
     */
    public abstract void execute() throws ScheduledExecutionException;

    public abstract String toString();

    public static enum TaskStatus
    {
        STARTED,
        COMPLETED,
        FAILED
    }
}
