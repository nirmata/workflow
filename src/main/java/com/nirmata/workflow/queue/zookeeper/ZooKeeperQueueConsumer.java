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

package com.nirmata.workflow.queue.zookeeper;

import com.google.common.base.Preconditions;
import com.nirmata.workflow.details.WorkflowManagerImpl;
import com.nirmata.workflow.details.ZooKeeperConstants;
import com.nirmata.workflow.models.ExecutableTask;
import com.nirmata.workflow.models.TaskType;
import com.nirmata.workflow.queue.QueueConsumer;
import com.nirmata.workflow.queue.TaskRunner;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.queue.QueueBuilder;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.utils.CloseableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.atomic.AtomicBoolean;

public class ZooKeeperQueueConsumer implements QueueConsumer, org.apache.curator.framework.recipes.queue.QueueConsumer<ExecutableTask>
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final InternalQueueBase queue;
    private final TaskRunner taskRunner;
    private final AtomicBoolean isOpen = new AtomicBoolean(false);

    public ZooKeeperQueueConsumer(WorkflowManagerImpl workflowManager, TaskRunner taskRunner, TaskType taskType)
    {
        this.taskRunner = Preconditions.checkNotNull(taskRunner, "taskRunner cannot be null");
        workflowManager = Preconditions.checkNotNull(workflowManager, "workflowManager cannot be null");
        String path = ZooKeeperConstants.getQueuePath(taskType);
        QueueBuilder<ExecutableTask> builder = QueueBuilder.builder(workflowManager.getCurator(), this, new TaskQueueSerializer(workflowManager.getSerializer()), path);
        if ( taskType.isIdempotent() )
        {
            builder = builder.lockPath(ZooKeeperConstants.getQueueLockPath(taskType));
        }
        queue = ZooKeeperQueue.makeQueue(builder, taskType);
    }

    @Override
    public void start()
    {
        isOpen.set(true);
        try
        {
            queue.start();
        }
        catch ( Exception e )
        {
            log.error("Could not start queue", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close()
    {
        isOpen.set(false);
        CloseableUtils.closeQuietly(queue);
    }

    @Override
    public void consumeMessage(ExecutableTask executableTask)
    {
        if ( isOpen.get() )
        {
            taskRunner.executeTask(executableTask);
        }
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState)
    {
        // other parts of the library will handle
    }
}
