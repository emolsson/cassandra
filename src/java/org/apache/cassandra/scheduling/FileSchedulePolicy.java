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

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@link ISchedulePolicy} that uses a local configuration file to restrict when
 * {@link ScheduledJob ScheduledJobs} can run.
 */
public class FileSchedulePolicy implements ISchedulePolicy
{
    private static final Logger logger = LoggerFactory.getLogger(FileSchedulePolicy.class);

    public static final String SCHEDULE_PROPERTIES_FILENAME = "schedule_policy.properties";

    private final List<TimeRange> timeRanges = new ArrayList<>();

    public FileSchedulePolicy()
    {
        loadConfig();
    }

    @Override
    public long validate(ScheduledJob job)
    {
        int secondOfDay = DateTime.now().getSecondOfDay();

        for (TimeRange range : timeRanges)
        {
            if (range.intersects(secondOfDay))
                return range.timeToEnd(secondOfDay);
        }

        return -1;
    }

    private void loadConfig() throws ConfigurationException
    {
        Properties properties = new Properties();

        try (InputStream stream = getClass().getClassLoader().getResourceAsStream(SCHEDULE_PROPERTIES_FILENAME))
        {
            properties.load(stream);
        }
        catch (Exception e)
        {
            throw new ConfigurationException("Unable to read " + SCHEDULE_PROPERTIES_FILENAME, e);
        }

        for (Map.Entry<Object, Object> entry : properties.entrySet())
        {
            assert entry.getKey() instanceof String && entry.getValue() instanceof String;

            String key = (String) entry.getKey();
            String value = (String) entry.getValue();

            if ("deny".equals(key))
            {
                String[] ranges = value.split(",");

                for (String range : ranges)
                {
                    timeRanges.add(new TimeRange(range));
                }
            }
            else
            {
                logger.warn("Unknown configuration: {}", key);
            }
        }
    }

    private static class TimeRange
    {
        private static final int endOfDay = 86400;

        private static final Pattern rangePattern = Pattern.compile("([0-9]{2}):([0-9]{2})-([0-9]{2}):([0-9]{2})");

        private final int start;
        private final int end;

        private TimeRange(String range)
        {
            Matcher matcher = rangePattern.matcher(range);

            if (!matcher.matches())
                throw new ConfigurationException(String.format(
                        "Invalid time range: '%s' should be defined as 'hh:mm-hh:mm'", range));

            String startHour = matcher.group(1);
            String startMinute = matcher.group(2);
            String endHour = matcher.group(3);
            String endMinute = matcher.group(4);

            this.start = timeToSeconds(startHour, startMinute);
            this.end = timeToSeconds(endHour, endMinute);

            logger.info("Denying scheduling from '{}:{}' to '{}:{}'", startHour, startMinute, endHour, endMinute);
        }

        /**
         * Check if the provided second of the day is intersecting with this TimeRange.
         *
         * @param secondOfDay
         * @return True if intersecting.
         */
        public boolean intersects(int secondOfDay)
        {
            if (wrapping())
                return Integer.compare(secondOfDay, start) >= 0 || Integer.compare(secondOfDay, end) <= 0;
            else
                return Integer.compare(secondOfDay, start) >= 0 && Integer.compare(secondOfDay, end) <= 0;
        }

        /**
         * Calculate the time in milliseconds until the end of the range based on the provided second of the day.
         *
         * @param secondOfDay
         * @return The calculated time.
         */
        public long timeToEnd(int secondOfDay)
        {
            if (wrapping())
            {
                return (endOfDay - secondOfDay + end) * 1000;
            }
            else
            {
                return (end - secondOfDay) * 1000;
            }
        }

        private boolean wrapping()
        {
            return end < start;
        }

        private int timeToSeconds(String hours, String minutes)
        {
            return Integer.parseInt(hours) * 3600 + Integer.parseInt(minutes) * 60;
        }
    }
}
