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
package com.nirmata.workflow.queue;

import com.nirmata.workflow.WorkflowManager;
import com.nirmata.workflow.models.TaskType;

public interface QueueFactory {
    Queue createQueue(WorkflowManager workflowManager, TaskType taskType);

    QueueConsumer createQueueConsumer(WorkflowManager workflowManager, TaskRunner taskRunner, TaskType taskType);

    /**
     * This can return a queue consumer that internally creates multiple task
     * executors. Else, by default, each queue consumer has only one runner
     */
    default QueueConsumer createQueueConsumer(WorkflowManager workflowManager, TaskRunner taskRunner, TaskType taskType,
            int qty) {
        return this.createQueueConsumer(workflowManager, taskRunner, taskType);
    }
}
