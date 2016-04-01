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
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class TestWorkflowListenerManager extends BaseForTests
{
    @Test
    public void testBasic() throws Exception
    {
        WorkflowListenerManager workflowListenerManager = null;
        TestTaskExecutor taskExecutor = new TestTaskExecutor(6);
        TaskType taskType = new TaskType("test", "1", true);
        WorkflowManager workflowManager = WorkflowManagerBuilder.builder()
            .addingTaskExecutor(taskExecutor, 10, taskType)
            .withCurator(curator, "test", "1")
            .build();
        try
        {
            Task task = new Task(new TaskId(), taskType);

            BlockingQueue<WorkflowEvent> eventQueue = Queues.newLinkedBlockingQueue();
            workflowListenerManager = workflowManager.newWorkflowListenerManager();
            workflowListenerManager.getListenable().addListener(eventQueue::add);

            workflowManager.start();
            workflowListenerManager.start();

            RunId runId = workflowManager.submitTask(task);

            timing.sleepABit();

            WorkflowEvent runStarted = new WorkflowEvent(WorkflowEvent.EventType.RUN_STARTED, runId);
            WorkflowEvent taskStarted = new WorkflowEvent(WorkflowEvent.EventType.TASK_STARTED, runId, task.getTaskId());
            WorkflowEvent event1 = eventQueue.poll(timing.milliseconds(), TimeUnit.MILLISECONDS);
            WorkflowEvent event2 = eventQueue.poll(timing.milliseconds(), TimeUnit.MILLISECONDS);
            // due to timing, task start might come first
            Assert.assertTrue((event1.equals(runStarted) && event2.equals(taskStarted)) || (event2.equals(runStarted) && event1.equals(taskStarted)));

            Assert.assertEquals(eventQueue.poll(timing.milliseconds(), TimeUnit.MILLISECONDS), new WorkflowEvent(WorkflowEvent.EventType.TASK_COMPLETED, runId, task.getTaskId()));
            Assert.assertEquals(eventQueue.poll(timing.milliseconds(), TimeUnit.MILLISECONDS), new WorkflowEvent(WorkflowEvent.EventType.RUN_UPDATED, runId));
        }
        finally
        {
            CloseableUtils.closeQuietly(workflowListenerManager);
            CloseableUtils.closeQuietly(workflowManager);
        }
    }
}
