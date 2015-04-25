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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.io.Resources;
import com.nirmata.workflow.admin.StandardAutoCleaner;
import com.nirmata.workflow.executor.TaskExecution;
import com.nirmata.workflow.executor.TaskExecutionStatus;
import com.nirmata.workflow.executor.TaskExecutor;
import com.nirmata.workflow.models.ExecutableTask;
import com.nirmata.workflow.models.RunId;
import com.nirmata.workflow.models.Task;
import com.nirmata.workflow.models.TaskExecutionResult;
import com.nirmata.workflow.models.TaskId;
import com.nirmata.workflow.models.TaskType;
import com.nirmata.workflow.serialization.JsonSerializerMapper;
import org.apache.curator.utils.CloseableUtils;
import org.testng.annotations.Test;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static com.nirmata.workflow.WorkflowAssertions.assertThat;

public class TestNormal extends BaseForTests
{
    @Test
    public void testFailedStop() throws Exception
    {
        TestTaskExecutor taskExecutor = new TestTaskExecutor(2)
        {
            @Override
            public TaskExecution newTaskExecution(WorkflowManager workflowManager, ExecutableTask task)
            {
                if ( task.getTaskId().getId().equals("task3") )
                {
                    return () -> new TaskExecutionResult(TaskExecutionStatus.FAILED_STOP, "stop");
                }
                return super.newTaskExecution(workflowManager, task);
            }
        };
        TaskType taskType = new TaskType("test", "1", true);
        try (WorkflowManager workflowManager = WorkflowManagerBuilder.builder()
                .addingTaskExecutor(taskExecutor, 10, taskType)
                .withCurator(curator, "test", "1")
                .build())
        {
            workflowManager.start();

            Task task4 = new Task(new TaskId("task4"), taskType);
            Task task3 = new Task(new TaskId("task3"), taskType, Lists.newArrayList(task4));
            Task task2 = new Task(new TaskId("task2"), taskType, Lists.newArrayList(task3));
            Task task1 = new Task(new TaskId("task1"), taskType, Lists.newArrayList(task2));
            RunId runId = workflowManager.submitTask(task1);

            assertThat(timing.awaitLatch(taskExecutor.latch)).isTrue();
            timing.sleepABit(); // make sure other tasks are not started

            assertThat(workflowManager.getAdmin().getRunInfo(runId)).isComplete();

            taskExecutor.checker.assertTasksSetsContainOnly(
                    ImmutableSet.of(new TaskId("task1")),
                    ImmutableSet.of(new TaskId("task2"))
            );
        }
    }

    @Test
    public void testAutoCleanRun() throws Exception
    {
        TaskExecutor taskExecutor = (w, t) -> () -> new TaskExecutionResult(TaskExecutionStatus.SUCCESS, "");
        TaskType taskType = new TaskType("test", "1", true);
        try (WorkflowManager workflowManager = WorkflowManagerBuilder.builder()
                .addingTaskExecutor(taskExecutor, 10, taskType)
                .withCurator(curator, "test", "1")
                .withAutoCleaner(new StandardAutoCleaner(Duration.ofMillis(1)), Duration.ofMillis(1))
                .build())
        {
            workflowManager.start();

            Task task2 = new Task(new TaskId(), taskType);
            Task task1 = new Task(new TaskId(), taskType, Lists.newArrayList(task2));
            workflowManager.submitTask(task1);

            timing.sleepABit();

            assertThat(workflowManager.getAdmin().getRunInfo()).isEmpty(); // asserts that the cleaner ran
        }
    }

    @Test
    public void testCanceling() throws Exception
    {
        Semaphore executionLatch = new Semaphore(0);
        CountDownLatch continueLatch = new CountDownLatch(1);

        TaskExecutor taskExecutor = (w, t) -> () -> {
            executionLatch.release();
            try
            {
                continueLatch.await();
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
            }
            return new TaskExecutionResult(TaskExecutionStatus.SUCCESS, "");
        };
        TaskType taskType = new TaskType("test", "1", true);
        try (WorkflowManager workflowManager = WorkflowManagerBuilder.builder()
                .addingTaskExecutor(taskExecutor, 10, taskType)
                .withCurator(curator, "test", "1")
                .build())
        {
            workflowManager.start();

            Task task2 = new Task(new TaskId(), taskType);
            Task task1 = new Task(new TaskId(), taskType, Lists.newArrayList(task2));
            RunId runId = workflowManager.submitTask(task1);

            assertThat(timing.acquireSemaphore(executionLatch, 1)).isTrue();

            workflowManager.cancelRun(runId);
            continueLatch.countDown();

            assertThat(executionLatch.tryAcquire(1, 5, TimeUnit.SECONDS)).isFalse();  // no more executions should occur
        }
    }

    @Test
    public void testSingleClientSimple() throws Exception
    {
        TestTaskExecutor taskExecutor = new TestTaskExecutor(6);
        try (WorkflowManager workflowManager = WorkflowManagerBuilder.builder()
                .addingTaskExecutor(taskExecutor, 10, new TaskType("test", "1", true))
                .withCurator(curator, "test", "1")
                .build())
        {
            workflowManager.start();

            String json = Resources.toString(Resources.getResource("tasks.json"), Charset.defaultCharset());
            JsonSerializerMapper jsonSerializerMapper = new JsonSerializerMapper();
            Task task = jsonSerializerMapper.get(jsonSerializerMapper.getMapper().readTree(json), Task.class);
            workflowManager.submitTask(task);

            assertThat(timing.awaitLatch(taskExecutor.latch)).isTrue();

            taskExecutor.checker.assertTasksSetsContainOnly(
                    ImmutableSet.of(new TaskId("task1"), new TaskId("task2")),
                    ImmutableSet.of(new TaskId("task3"), new TaskId("task4"), new TaskId("task5")),
                    ImmutableSet.of(new TaskId("task6"))
            );
        }
    }

    @Test
    public void testMultiClientSimple() throws Exception
    {
        final int QTY = 4;

        TestTaskExecutor taskExecutor = new TestTaskExecutor(6);
        TaskType taskType = new TaskType("test", "1", true);
        List<WorkflowManager> workflowManagers = Lists.newArrayList();
        for ( int i = 0; i < QTY; ++i )
        {
            WorkflowManager workflowManager = WorkflowManagerBuilder.builder()
                .addingTaskExecutor(taskExecutor, 10, taskType)
                .withCurator(curator, "test", "1")
                .build();
            workflowManagers.add(workflowManager);
        }
        try
        {
            workflowManagers.forEach(WorkflowManager::start);

            String json = Resources.toString(Resources.getResource("tasks.json"), Charset.defaultCharset());
            JsonSerializerMapper jsonSerializerMapper = new JsonSerializerMapper();
            Task task = jsonSerializerMapper.get(jsonSerializerMapper.getMapper().readTree(json), Task.class);
            workflowManagers.get(QTY - 1).submitTask(task);

            assertThat(timing.awaitLatch(taskExecutor.latch)).isTrue();

            taskExecutor.checker.assertTasksSetsContainOnly(
                    ImmutableSet.of(new TaskId("task1"), new TaskId("task2")),
                    ImmutableSet.of(new TaskId("task3"), new TaskId("task4"), new TaskId("task5")),
                    ImmutableSet.of(new TaskId("task6"))
            );
        } finally
        {
            workflowManagers.forEach(CloseableUtils::closeQuietly);
        }
    }

    @Test
    public void testNoData() throws Exception
    {
        try (WorkflowManager workflowManager = WorkflowManagerBuilder.builder()
                .addingTaskExecutor(new TestTaskExecutor(1), 10, new TaskType("test", "1", true))
                .withCurator(curator, "test", "1")
                .build())
        {
            Optional<TaskExecutionResult> taskData = workflowManager.getTaskExecutionResult(new RunId(), new TaskId());
            assertThat(taskData).isEmpty();
        }
    }

    @Test
    public void testTaskData() throws Exception
    {
        CountDownLatch latch = new CountDownLatch(1);
        TaskExecutor taskExecutor = (w, t) -> () -> {
            latch.countDown();
            Map<String, String> resultData = Maps.newHashMap();
            resultData.put("one", "1");
            resultData.put("two", "2");
            return new TaskExecutionResult(TaskExecutionStatus.SUCCESS, "test", resultData);
        };
        TaskType taskType = new TaskType("test", "1", true);
        try (WorkflowManager workflowManager = WorkflowManagerBuilder.builder()
                .addingTaskExecutor(taskExecutor, 10, taskType)
                .withCurator(curator, "test", "1")
                .build())
        {
            workflowManager.start();

            TaskId taskId = new TaskId();
            RunId runId = workflowManager.submitTask(new Task(taskId, taskType));

            assertThat(timing.awaitLatch(latch)).isTrue();
            timing.sleepABit();

            Optional<TaskExecutionResult> taskData = workflowManager.getTaskExecutionResult(runId, taskId);
            assertThat(taskData).isPresent();

            assertThat(taskData.get().getResultData())
                    .hasSize(2)
                    .containsEntry("one", "1")
                    .containsEntry("two", "2");
        }
    }

    @Test
    public void testSubTask() throws Exception
    {
        TaskType taskType = new TaskType("test", "1", true);
        Task groupAChild = new Task(new TaskId(), taskType);
        Task groupAParent = new Task(new TaskId(), taskType, Lists.newArrayList(groupAChild));

        Task groupBTask = new Task(new TaskId(), taskType);

        BlockingQueue<TaskId> tasks = Queues.newLinkedBlockingQueue();
        CountDownLatch latch = new CountDownLatch(1);
        TaskExecutor taskExecutor = (workflowManager, task) -> () -> {
            tasks.add(task.getTaskId());
            if ( task.getTaskId().equals(groupBTask.getTaskId()) )
            {
                try
                {
                    latch.await();
                }
                catch ( InterruptedException e )
                {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException();
                }
            }
            RunId subTaskRunId = task.getTaskId().equals(groupAParent.getTaskId()) ? workflowManager.submitSubTask(task.getRunId(), groupBTask) : null;
            return new TaskExecutionResult(TaskExecutionStatus.SUCCESS, "test", Maps.newLinkedHashMap(), subTaskRunId);
        };
        try (WorkflowManager workflowManager = WorkflowManagerBuilder.builder()
                .addingTaskExecutor(taskExecutor, 10, taskType)
                .withCurator(curator, "test", "1")
                .build())
        {
            workflowManager.start();
            workflowManager.submitTask(groupAParent);

            assertThat(poll(tasks)).isEqualTo(groupAParent.getTaskId());
            assertThat(poll(tasks)).isEqualTo(groupBTask.getTaskId());
            timing.sleepABit();
            assertThat(tasks).isEmpty();

            latch.countDown();
            assertThat(poll(tasks)).isEqualTo(groupAChild.getTaskId());
        }
    }

    @Test
    public void testMultiTypesExecution() throws Exception
    {
        TaskType taskType1 = new TaskType("type1", "1", true);
        TaskType taskType2 = new TaskType("type2", "1", true);
        TaskType taskType3 = new TaskType("type3", "1", true);

        TestTaskExecutor taskExecutor = new TestTaskExecutor(6);
        try (WorkflowManager workflowManager = WorkflowManagerBuilder.builder()
                .addingTaskExecutor(taskExecutor, 10, taskType1)
                .addingTaskExecutor(taskExecutor, 10, taskType2)
                .addingTaskExecutor(taskExecutor, 10, taskType3)
                .withCurator(curator, "test", "1")
                .build())
        {
            workflowManager.start();

            String json = Resources.toString(Resources.getResource("multi-tasks.json"), Charset.defaultCharset());
            JsonSerializerMapper jsonSerializerMapper = new JsonSerializerMapper();
            Task task = jsonSerializerMapper.get(jsonSerializerMapper.getMapper().readTree(json), Task.class);
            workflowManager.submitTask(task);

            assertThat(timing.awaitLatch(taskExecutor.latch)).isTrue();

            taskExecutor.checker.assertTasksSetsContainOnly(
                    ImmutableSet.of(new TaskId("task1"), new TaskId("task2")),
                    ImmutableSet.of(new TaskId("task3"), new TaskId("task4"), new TaskId("task5")),
                    ImmutableSet.of(new TaskId("task6"))
            );
        }
    }

    @Test
    public void testMultiTypes() throws Exception
    {
        TaskType taskType1 = new TaskType("type1", "1", true);
        TaskType taskType2 = new TaskType("type2", "1", true);
        TaskType taskType3 = new TaskType("type3", "1", true);

        BlockingQueue<TaskId> queue1 = Queues.newLinkedBlockingQueue();
        TaskExecutor taskExecutor1 = (manager, task) -> () -> {
            queue1.add(task.getTaskId());
            return new TaskExecutionResult(TaskExecutionStatus.SUCCESS, "");
        };

        BlockingQueue<TaskId> queue2 = Queues.newLinkedBlockingQueue();
        TaskExecutor taskExecutor2 = (manager, task) -> () -> {
            queue2.add(task.getTaskId());
            return new TaskExecutionResult(TaskExecutionStatus.SUCCESS, "");
        };

        BlockingQueue<TaskId> queue3 = Queues.newLinkedBlockingQueue();
        TaskExecutor taskExecutor3 = (manager, task) -> () -> {
            queue3.add(task.getTaskId());
            return new TaskExecutionResult(TaskExecutionStatus.SUCCESS, "");
        };

        try (WorkflowManager workflowManager = WorkflowManagerBuilder.builder()
                .addingTaskExecutor(taskExecutor1, 10, taskType1)
                .addingTaskExecutor(taskExecutor2, 10, taskType2)
                .addingTaskExecutor(taskExecutor3, 10, taskType3)
                .withCurator(curator, "test", "1")
                .build())
        {
            workflowManager.start();

            String json = Resources.toString(Resources.getResource("multi-tasks.json"), Charset.defaultCharset());
            JsonSerializerMapper jsonSerializerMapper = new JsonSerializerMapper();
            Task task = jsonSerializerMapper.get(jsonSerializerMapper.getMapper().readTree(json), Task.class);
            workflowManager.submitTask(task);

            assertThat(Arrays.asList(poll(queue1), poll(queue1)))
                    .containsOnly(new TaskId("task1"), new TaskId("task2"));
            assertThat(Arrays.asList(poll(queue2), poll(queue2)))
                    .containsOnly(new TaskId("task3"), new TaskId("task4"));
            assertThat(Arrays.asList(poll(queue3), poll(queue3)))
                    .containsOnly(new TaskId("task5"), new TaskId("task6"));

            timing.sleepABit();

            assertThat(queue1).isEmpty();
            assertThat(queue2).isEmpty();
            assertThat(queue3).isEmpty();
        }
    }
}
