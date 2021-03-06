/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db.commitlog;

import java.io.File;
import java.io.IOException;

import com.google.common.base.Predicate;

import org.junit.Assert;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.NIODataInputStream;

/**
 * Utility class for tests needing to examine the commitlog contents.
 */
public class CommitLogTestReplayer extends CommitLogReplayer
{
    public static void examineCommitLog(Predicate<Mutation> processor) throws IOException
    {
        CommitLog.instance.sync(true);

        CommitLogTestReplayer replayer = new CommitLogTestReplayer(processor);
        File commitLogDir = new File(DatabaseDescriptor.getCommitLogLocation());
        replayer.recover(commitLogDir.listFiles());
    }

    final private Predicate<Mutation> processor;

    public CommitLogTestReplayer(Predicate<Mutation> processor)
    {
        this(ReplayPosition.NONE, processor);
    }

    public CommitLogTestReplayer(ReplayPosition discardedPos, Predicate<Mutation> processor)
    {
        super(discardedPos, null, ReplayFilter.create());
        this.processor = processor;
    }

    @Override
    void replayMutation(byte[] inputBuffer, int size, final long entryLocation, final CommitLogDescriptor desc)
    {
        NIODataInputStream bufIn = new DataInputBuffer(inputBuffer, 0, size);
        Mutation mutation;
        try
        {
            mutation = Mutation.serializer.deserialize(bufIn,
                                                           desc.getMessagingVersion(),
                                                           SerializationHelper.Flag.LOCAL);
            Assert.assertTrue(processor.apply(mutation));
        }
        catch (IOException e)
        {
            // Test fails.
            throw new AssertionError(e);
        }
    }
}
