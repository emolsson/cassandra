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
package org.apache.cassandra.schema;

import static java.lang.String.format;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.repair.RepairParallelism;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.MoreObjects;

public class RepairSchedulingParams
{
    public static IVersionedSerializer<RepairSchedulingParams> serializer = new RepairSchedulingParamsSerializer();

    private static final Logger logger = LoggerFactory.getLogger(RepairSchedulingParams.class);

    public enum Option
    {
        ENABLED,
        INCREMENTAL,
        MIN_DELAY,
        PARALLELISM;

        @Override
        public String toString()
        {
            return name().toLowerCase();
        }
    }

    /**
     * 1 day
     */
    private static final long DEFAULT_MIN_DELAY = 86400;
    private static final boolean DEFAULT_INCREMENTAL = true;
    public static final RepairParallelism DEFAULT_PARALLELISM = RepairParallelism.SEQUENTIAL;

    public static final RepairSchedulingParams DEFAULT = new RepairSchedulingParams(false, DEFAULT_INCREMENTAL, DEFAULT_MIN_DELAY, DEFAULT_PARALLELISM);

    private final boolean enabled;
    private final boolean incremental;
    private final long minDelay;
    private final RepairParallelism parallelism;

    /**
     * @param opts
     * @param cfMetaData
     * @return
     */
    public static RepairSchedulingParams fromMap(Map<String, String> opts)
    {
        Map<String, String> options = copyOptions(opts);

        boolean enabled = removeEnabled(options);
        boolean incremental = removeIncremental(options);
        long minDelay = -1;

        if (!hasMinDelay(options))
        {
            if (!incremental)
            {
                logger.info("Setting 'min_delay' to the default gc grace seconds of {}, if this is incorrect please specify it directly.");
                minDelay = TableParams.DEFAULT_GC_GRACE_SECONDS / 2;
            }
        }

        if (minDelay == -1)
        {
            minDelay = removeMinDelay(options);
        }

        RepairParallelism parallelism = removeParallelism(options);

        if (!options.isEmpty())
            throw new ConfigurationException(format("Invalid repair scheduling sub-options %s", options.keySet()));

        return new RepairSchedulingParams(enabled, incremental, minDelay, parallelism);
    }

    private static boolean removeEnabled(Map<String, String> options)
    {
        String enabled = options.remove(Option.ENABLED.toString());
        return enabled == null || Boolean.parseBoolean(enabled);
    }

    private static boolean hasMinDelay(Map<String, String> options)
    {
        String minDelay = options.get(Option.MIN_DELAY.toString());
        return minDelay != null;
    }

    private static long removeMinDelay(Map<String, String> options)
    {
        String minDelay = options.remove(Option.MIN_DELAY.toString());
        return minDelay == null ? DEFAULT_MIN_DELAY : Long.parseLong(minDelay);
    }

    private static boolean removeIncremental(Map<String, String> options)
    {
        String incremental = options.remove(Option.INCREMENTAL.toString());
        return incremental == null ? DEFAULT_INCREMENTAL : Boolean.parseBoolean(incremental);
    }

    private static RepairParallelism removeParallelism(Map<String, String> options)
    {
        String parallelism = options.remove(Option.PARALLELISM.toString());
        return parallelism == null ? DEFAULT_PARALLELISM : RepairParallelism.fromName(parallelism);
    }

    private static Map<String, String> copyOptions(Map<? extends CharSequence, ? extends CharSequence> co)
    {
        if (co == null || co.isEmpty())
            return Collections.<String, String> emptyMap();

        Map<String, String> repairSchedulingOptions = new HashMap<>();
        for (Map.Entry<? extends CharSequence, ? extends CharSequence> entry : co.entrySet())
            repairSchedulingOptions.put(entry.getKey().toString(), entry.getValue().toString());
        return repairSchedulingOptions;
    }

    public RepairSchedulingParams(boolean enabled, boolean incremental, long minDelay, RepairParallelism parallelism)
    {
        this.enabled = enabled;
        this.incremental = incremental;
        this.minDelay = minDelay;
        this.parallelism = parallelism;
    }

    public boolean enabled()
    {
        return enabled;
    }

    public boolean incremental()
    {
        return incremental;
    }

    public long minDelay()
    {
        return minDelay;
    }

    public RepairParallelism parallelism()
    {
        return parallelism;
    }

    public void validate() throws ConfigurationException
    {
        if (minDelay < 0)
            throw new ConfigurationException("Interval must be a positive value");
    }

    public Map<String, String> asMap()
    {
        Map<String, String> map = new HashMap<>();
        map.put(Option.ENABLED.toString(), Boolean.toString(enabled));
        map.put(Option.INCREMENTAL.toString(), Boolean.toString(incremental));
        map.put(Option.MIN_DELAY.toString(), Long.toString(minDelay));
        return map;
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("enabled", enabled)
                .add("incremental", incremental)
                .add("minDelay", minDelay)
                .toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof RepairSchedulingParams))
            return false;

        RepairSchedulingParams rsp = (RepairSchedulingParams) o;

        return enabled == rsp.enabled && incremental == rsp.incremental && minDelay == rsp.minDelay;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(enabled, incremental, minDelay);
    }

    public static class RepairSchedulingParamsSerializer implements IVersionedSerializer<RepairSchedulingParams>
    {

        @Override
        public void serialize(RepairSchedulingParams t, DataOutputPlus out, int version) throws IOException
        {
            Map<String, String> params = t.asMap();
            out.writeInt(params.size());
            for (Map.Entry<String, String> entry : params.entrySet())
            {
                out.writeUTF(entry.getKey());
                out.writeUTF(entry.getValue());
            }
        }

        @Override
        public RepairSchedulingParams deserialize(DataInputPlus in, int version) throws IOException
        {
            Map<String, String> params = new HashMap<>();

            int entries = in.readInt();

            for (int i = 0; i < entries; i++)
            {
                String key = in.readUTF();
                String value = in.readUTF();
                params.put(key, value);
            }

            return fromMap(params);
        }

        @Override
        public long serializedSize(RepairSchedulingParams t, int version)
        {
            int size = 0;

            Map<String, String> params = t.asMap();

            size += TypeSizes.sizeof(params.size());
            for (Map.Entry<String, String> entry : params.entrySet())
            {
                size += TypeSizes.sizeof(entry.getKey());
                size += TypeSizes.sizeof(entry.getValue());
            }

            return size;
        }

    }
}
