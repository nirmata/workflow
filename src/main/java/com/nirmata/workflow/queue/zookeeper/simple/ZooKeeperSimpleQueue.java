/**
 * Copyright 2014 Nirmata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nirmata.workflow.queue.zookeeper.simple;

import com.google.common.base.Preconditions;
import com.nirmata.workflow.details.ZooKeeperConstants;
import com.nirmata.workflow.models.ExecutableTask;
import com.nirmata.workflow.models.TaskType;
import com.nirmata.workflow.queue.Queue;
import com.nirmata.workflow.queue.TaskRunner;
import com.nirmata.workflow.serialization.Serializer;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooKeeperSimpleQueue implements Queue
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final SimpleQueue queue;
    private final Serializer serializer;

    ZooKeeperSimpleQueue(TaskRunner taskRunner, Serializer serializer, CuratorFramework client, TaskType taskType)
    {
        this.serializer = Preconditions.checkNotNull(serializer, "serializer cannot be null");
        String path = ZooKeeperConstants.getQueuePath(taskType);
        String lockPath = ZooKeeperConstants.getQueueLockPath(taskType);
        queue = new SimpleQueue(client, taskRunner, serializer, path, lockPath, taskType.isIdempotent());
    }

    @Override
    public void start()
    {
        // NOP
    }

    @Override
    public void close()
    {
        // NOP
    }

    @Override
    public void put(ExecutableTask executableTask)
    {
        try
        {
            byte[] bytes = serializer.serialize(executableTask);
            queue.put(bytes);
        }
        catch ( Exception e )
        {
            log.error("Could not add to queue for: " + executableTask, e);
            throw new RuntimeException(e);
        }
    }

    SimpleQueue getQueue()
    {
        return queue;
    }

    Serializer getSerializer()
    {
        return serializer;
    }
}
