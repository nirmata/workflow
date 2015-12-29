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
import com.nirmata.workflow.details.WorkflowManagerImpl;
import com.nirmata.workflow.models.TaskMode;
import com.nirmata.workflow.models.TaskType;
import com.nirmata.workflow.queue.Queue;
import com.nirmata.workflow.queue.QueueConsumer;
import com.nirmata.workflow.queue.QueueFactory;
import com.nirmata.workflow.queue.TaskRunner;

public class ZooKeeperSimpleQueueFactory implements QueueFactory
{
    @Override
    public Queue createQueue(WorkflowManagerImpl workflowManager, TaskType taskType)
    {
        return internalCreateQueue(workflowManager, taskType, null);
    }

    @Override
    public QueueConsumer createQueueConsumer(WorkflowManagerImpl workflowManager, TaskRunner taskRunner, TaskType taskType)
    {
        ZooKeeperSimpleQueue queue = internalCreateQueue(workflowManager, taskType, taskRunner);
        return queue.getQueue();
    }

    private ZooKeeperSimpleQueue internalCreateQueue(WorkflowManagerImpl workflowManager, TaskType taskType, TaskRunner taskRunner)
    {
        Preconditions.checkArgument(taskType.getMode() == TaskMode.STANDARD, "ZooKeeperSimpleQueueFactory only supports TaskMode.STANDARD");
        return new ZooKeeperSimpleQueue(taskRunner, workflowManager.getSerializer(), workflowManager.getCurator(), taskType);
    }
}
