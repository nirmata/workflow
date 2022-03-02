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
package com.nirmata.workflow.queue.kafka;

import com.nirmata.workflow.WorkflowManager;
import com.nirmata.workflow.details.WorkflowManagerKafkaImpl;
import com.nirmata.workflow.models.TaskType;
import com.nirmata.workflow.queue.Queue;
import com.nirmata.workflow.queue.QueueConsumer;
import com.nirmata.workflow.queue.QueueFactory;
import com.nirmata.workflow.queue.TaskRunner;

public class KafkaSimpleQueueFactory implements QueueFactory {

    @Override
    public Queue createQueue(WorkflowManager workflowManager, TaskType taskType) {
        return internalCreateQueue((WorkflowManagerKafkaImpl) workflowManager, taskType, null, 1);
    }

    @Override
    public QueueConsumer createQueueConsumer(WorkflowManager workflowManager, TaskRunner taskRunner,
            TaskType taskType) {
        KafkaSimpleQueue queue = internalCreateQueue((WorkflowManagerKafkaImpl) workflowManager, taskType, taskRunner,
                1);
        return queue.getQueue();
    }

    @Override
    public QueueConsumer createQueueConsumer(WorkflowManager workflowManager, TaskRunner taskRunner,
            TaskType taskType, int qty) {
        KafkaSimpleQueue queue = internalCreateQueue((WorkflowManagerKafkaImpl) workflowManager, taskType, taskRunner,
                qty);
        return queue.getQueue();
    }

    private KafkaSimpleQueue internalCreateQueue(WorkflowManagerKafkaImpl workflowManager, TaskType taskType,
            TaskRunner taskRunner, int qty) {
        return new KafkaSimpleQueue(taskRunner, workflowManager.getSerializer(), workflowManager.getKafkaConf(),
                taskType, qty);
    }
}
