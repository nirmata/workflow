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
package com.nirmata.workflow.details;

import com.google.common.collect.Lists;
import com.nirmata.workflow.BaseForTests;
import com.nirmata.workflow.WorkflowManager;
import com.nirmata.workflow.WorkflowManagerBuilder;
import com.nirmata.workflow.executor.TaskExecutionStatus;
import com.nirmata.workflow.executor.TaskExecutor;
import com.nirmata.workflow.models.Task;
import com.nirmata.workflow.models.TaskExecutionResult;
import com.nirmata.workflow.models.TaskId;
import com.nirmata.workflow.models.TaskMode;
import com.nirmata.workflow.models.TaskType;
import com.nirmata.workflow.queue.zookeeper.SimpleQueue;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class TestDelayPriorityTasks extends BaseForTests
{
    @Test
    public void testDelay() throws Exception
    {
        final long delayMs = TimeUnit.SECONDS.toMillis(5);

        BlockingQueue<Long> queue = new LinkedBlockingQueue<>();
        TaskExecutor taskExecutor = (workflowManager, executableTask) -> () ->
        {
            queue.add(System.currentTimeMillis());
            return new TaskExecutionResult(TaskExecutionStatus.SUCCESS, "");
        };
        TaskType taskType = new TaskType("test", "1", true, TaskMode.DELAY);
        try ( WorkflowManager workflowManager = WorkflowManagerBuilder.builder()
            .addingTaskExecutor(taskExecutor, 10, taskType)
            .withCurator(curator, "test", "1")
            .build() )
        {
            workflowManager.start();

            Task task = new Task(new TaskId(), taskType);
            workflowManager.submitTask(task);

            Long ticksMs = queue.poll(1, TimeUnit.SECONDS);
            Assert.assertNotNull(ticksMs);
            Assert.assertTrue((ticksMs - ((WorkflowManagerImpl)workflowManager).debugLastSubmittedTimeMs) < 1000);  // should have executed immediately

            task = new Task(new TaskId(), taskType, Lists.newArrayList(), Task.makeSpecialMeta(System.currentTimeMillis() + delayMs));
            long startTicks = System.currentTimeMillis();
            workflowManager.submitTask(task);
            ticksMs = queue.poll(delayMs * 2, TimeUnit.MILLISECONDS);
            Assert.assertNotNull(ticksMs);
            long elapsed = ticksMs - startTicks;
            Assert.assertTrue(elapsed >= delayMs, String.format("Bad timing. Elapsed: %d, delay: %d ", elapsed, delayMs));
        }
    }

    @Test
    public void testPriority() throws Exception
    {
        BlockingQueue<String> queue = new LinkedBlockingQueue<>();
        TaskExecutor taskExecutor = (workflowManager, executableTask) -> () ->
        {
            queue.add(executableTask.getTaskId().getId());
            try
            {
                Thread.sleep(1);
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
            }
            return new TaskExecutionResult(TaskExecutionStatus.SUCCESS, "");
        };
        TaskType taskType = new TaskType("test", "1", true, TaskMode.PRIORITY);
        try ( WorkflowManager workflowManager = WorkflowManagerBuilder.builder()
            .addingTaskExecutor(taskExecutor, 1, taskType)
            .withCurator(curator, "test", "1")
            .build() )
        {
            SimpleQueue.debugQueuedTasks = new Semaphore(0);
            ((WorkflowManagerImpl)workflowManager).debugDontStartConsumers = true; // make sure all tasks are added to ZK before they start getting consumed
            workflowManager.start();

            Task task1 = new Task(new TaskId("1"), taskType, Lists.newArrayList(), Task.makeSpecialMeta(1));
            Task task2 = new Task(new TaskId("2"), taskType, Lists.newArrayList(), Task.makeSpecialMeta(100));
            Task task3 = new Task(new TaskId("3"), taskType, Lists.newArrayList(), Task.makeSpecialMeta(50));
            Task task4 = new Task(new TaskId("4"), taskType, Lists.newArrayList(), Task.makeSpecialMeta(300));
            Task task5 = new Task(new TaskId("5"), taskType, Lists.newArrayList(), Task.makeSpecialMeta(200));
            workflowManager.submitTask(task1);
            workflowManager.submitTask(task2);
            workflowManager.submitTask(task3);
            workflowManager.submitTask(task4);
            workflowManager.submitTask(task5);

            Assert.assertTrue(SimpleQueue.debugQueuedTasks.tryAcquire(5, 5, TimeUnit.SECONDS));
            ((WorkflowManagerImpl)workflowManager).startQueueConsumers();

            Assert.assertEquals(queue.poll(1, TimeUnit.SECONDS), "1");
            Assert.assertEquals(queue.poll(1, TimeUnit.SECONDS), "3");
            Assert.assertEquals(queue.poll(1, TimeUnit.SECONDS), "2");
            Assert.assertEquals(queue.poll(1, TimeUnit.SECONDS), "5");
            Assert.assertEquals(queue.poll(1, TimeUnit.SECONDS), "4");
        }
        finally
        {
            SimpleQueue.debugQueuedTasks = null;
        }
    }
}
