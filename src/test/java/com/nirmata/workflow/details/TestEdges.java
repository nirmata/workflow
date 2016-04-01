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
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.nirmata.workflow.BaseForTests;
import com.nirmata.workflow.WorkflowManager;
import com.nirmata.workflow.WorkflowManagerBuilder;
import com.nirmata.workflow.executor.TaskExecutionStatus;
import com.nirmata.workflow.executor.TaskExecutor;
import com.nirmata.workflow.models.Task;
import com.nirmata.workflow.models.TaskExecutionResult;
import com.nirmata.workflow.models.TaskId;
import com.nirmata.workflow.models.TaskType;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TestEdges extends BaseForTests
{
    @Test
    public void testMultiConcurrent() throws Exception
    {
        final int RETRIES = 3;

        try
        {
            IntStream.range(0, RETRIES).forEach(i -> {
                System.out.println("Retry " + i);
                multiConcurrentRetry();
            });
        }
        finally
        {
            Scheduler.debugBadRunIdCount = null;
        }
    }

    private void multiConcurrentRetry()
    {
        final int WORKFLOW_QTY = 3;
        final int ITERATIONS = 10;

        Scheduler.debugBadRunIdCount = new AtomicInteger(0);

        TaskType type1 = new TaskType("type1", "1", true);
        TaskType type2 = new TaskType("type2", "1", true);

        Semaphore semaphore = new Semaphore(0);
        TaskExecutor taskExecutor = (m, t) -> () -> {
            try
            {
                Thread.sleep(578);
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
            }
            semaphore.release();
            return new TaskExecutionResult(TaskExecutionStatus.SUCCESS, "");
        };

        List<WorkflowManager> workflowManagers = IntStream.range(0, WORKFLOW_QTY).mapToObj(i -> {
            TaskType type = ((i & 1) != 0) ? type1 : type2;
            return WorkflowManagerBuilder.builder()
                .addingTaskExecutor(taskExecutor, 10, type)
                .withCurator(curator, "test-" + i, "1")
                .build();
        }).collect(Collectors.toList());
        try
        {
            workflowManagers.stream().forEach(WorkflowManager::start);

            for ( int i = 0; i < ITERATIONS; ++i )
            {
                Assert.assertEquals(Scheduler.debugBadRunIdCount.get(), 0);
                System.out.println("Iteration " + i);
                for ( int j = 0; j < WORKFLOW_QTY; ++j )
                {
                    TaskType type = ((j & 1) != 0) ? type1 : type2;
                    workflowManagers.get(j).submitTask(new Task(new TaskId(), type));
                }
                Assert.assertTrue(timing.acquireSemaphore(semaphore, WORKFLOW_QTY));
            }
        }
        finally
        {
            workflowManagers.stream().forEach(CloseableUtils::closeQuietly);
        }
    }

    @Test
    public void testIdempotency() throws Exception
    {
        TaskType idempotentType = new TaskType("yes", "1", true);
        TaskType nonIdempotentType = new TaskType("no", "1", false);

        Task idempotentTask = new Task(new TaskId(), idempotentType);
        Task nonIdempotentTask = new Task(new TaskId(), nonIdempotentType);
        Task root = new Task(new TaskId(), Lists.newArrayList(idempotentTask, nonIdempotentTask));

        Set<TaskId> thrownTasks = Sets.newConcurrentHashSet();
        Queue<TaskId> tasks = Queues.newConcurrentLinkedQueue();
        TaskExecutor taskExecutor = (m, t) -> () -> {
            if ( thrownTasks.add(t.getTaskId()) )
            {
                throw new RuntimeException();
            }
            tasks.add(t.getTaskId());
            return new TaskExecutionResult(TaskExecutionStatus.SUCCESS, "");
        };
        WorkflowManager workflowManager = WorkflowManagerBuilder.builder()
            .addingTaskExecutor(taskExecutor, 10, idempotentType)
            .addingTaskExecutor(taskExecutor, 10, nonIdempotentType)
            .withCurator(curator, "test", "1")
            .build();
        try
        {
            workflowManager.start();
            workflowManager.submitTask(root);

            timing.sleepABit();

            Assert.assertEquals(tasks.size(), 1);
            Assert.assertEquals(tasks.poll(), idempotentTask.getTaskId());
        }
        finally
        {
            CloseableUtils.closeQuietly(workflowManager);
        }
    }
}
