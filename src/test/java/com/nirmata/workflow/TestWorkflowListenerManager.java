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
package com.nirmata.workflow;

import com.google.common.collect.Queues;
import com.nirmata.workflow.events.WorkflowEvent;
import com.nirmata.workflow.events.WorkflowListenerManager;
import com.nirmata.workflow.models.RunId;
import com.nirmata.workflow.models.Task;
import com.nirmata.workflow.models.TaskId;
import com.nirmata.workflow.models.TaskType;
import org.testng.annotations.Test;
import java.util.concurrent.BlockingQueue;

import static org.assertj.core.api.Assertions.assertThat;

public class TestWorkflowListenerManager extends BaseForTests
{
    @Test
    public void testBasic() throws Exception
    {
        TestTaskExecutor taskExecutor = new TestTaskExecutor(6);
        TaskType taskType = new TaskType("test", "1", true);
        try (WorkflowManager workflowManager = WorkflowManagerBuilder.builder()
                .addingTaskExecutor(taskExecutor, 10, taskType)
                .withCurator(curator, "test", "1")
                .build();
             WorkflowListenerManager workflowListenerManager = workflowManager.newWorkflowListenerManager())
        {
            Task task = new Task(new TaskId(), taskType);

            BlockingQueue<WorkflowEvent> eventQueue = Queues.newLinkedBlockingQueue();
            workflowListenerManager.getListenable().addListener(eventQueue::add);

            workflowManager.start();
            workflowListenerManager.start();

            RunId runId = workflowManager.submitTask(task);

            timing.sleepABit();

            assertThat(poll(eventQueue))
                    .isEqualTo(new WorkflowEvent(WorkflowEvent.EventType.RUN_STARTED, runId));
            assertThat(poll(eventQueue))
                    .isEqualTo(new WorkflowEvent(WorkflowEvent.EventType.TASK_STARTED, runId, task.getTaskId()));
            assertThat(poll(eventQueue))
                    .isEqualTo(new WorkflowEvent(WorkflowEvent.EventType.TASK_COMPLETED, runId, task.getTaskId()));
            assertThat(poll(eventQueue))
                    .isEqualTo(new WorkflowEvent(WorkflowEvent.EventType.RUN_UPDATED, runId));
        }
    }
}
