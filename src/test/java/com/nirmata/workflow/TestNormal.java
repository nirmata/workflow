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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import com.nirmata.workflow.admin.RunInfo;
import com.nirmata.workflow.admin.StandardAutoCleaner;
import com.nirmata.workflow.admin.TaskInfo;
import com.nirmata.workflow.details.WorkflowManagerImpl;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class TestNormal extends BaseForTests
{
    private final Logger log = LoggerFactory.getLogger(getClass());

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
        WorkflowManager workflowManager = WorkflowManagerBuilder.builder()
            .addingTaskExecutor(taskExecutor, 10, taskType)
            .withCurator(curator, "test", "1")
            .build();
        try
        {
            workflowManager.start();

            Task task4 = new Task(new TaskId("task4"), taskType);
            Task task3 = new Task(new TaskId("task3"), taskType, Lists.newArrayList(task4));
            Task task2 = new Task(new TaskId("task2"), taskType, Lists.newArrayList(task3));
            Task task1 = new Task(new TaskId("task1"), taskType, Lists.newArrayList(task2));
            RunId runId = workflowManager.submitTask(task1);

            Assert.assertTrue(timing.awaitLatch(taskExecutor.getLatch()));
            timing.sleepABit(); // make sure other tasks are not started

            RunInfo runInfo = workflowManager.getAdmin().getRunInfo(runId);
            Assert.assertTrue(runInfo.isComplete());

            List<Set<TaskId>> sets = taskExecutor.getChecker().getSets();
            List<Set<TaskId>> expectedSets = Arrays.<Set<TaskId>>asList
                (
                    Sets.newHashSet(new TaskId("task1")),
                    Sets.newHashSet(new TaskId("task2"))
                );
            Assert.assertEquals(sets, expectedSets);
        }
        finally
        {
            closeWorkflow(workflowManager);
        }
    }

    @Test
    public void testAutoCleanRun() throws Exception
    {

        TaskExecutor taskExecutor = (w, t) -> () -> new TaskExecutionResult(TaskExecutionStatus.SUCCESS, "");
        TaskType taskType = new TaskType("test", "1", true);
        WorkflowManager workflowManager = WorkflowManagerBuilder.builder()
            .addingTaskExecutor(taskExecutor, 10, taskType)
            .withCurator(curator, "test", "1")
            .withAutoCleaner(new StandardAutoCleaner(Duration.ofMillis(1)), Duration.ofMillis(1))
            .build();
        try
        {
            workflowManager.start();

            Task task2 = new Task(new TaskId(), taskType);
            Task task1 = new Task(new TaskId(), taskType, Lists.newArrayList(task2));
            workflowManager.submitTask(task1);

            timing.sleepABit();

            Assert.assertEquals(workflowManager.getAdmin().getRunInfo().size(), 0); // asserts that the cleaner ran
        }
        finally
        {
            closeWorkflow(workflowManager);
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
        WorkflowManager workflowManager = WorkflowManagerBuilder.builder()
            .addingTaskExecutor(taskExecutor, 10, taskType)
            .withCurator(curator, "test", "1")
            .build();
        try
        {
            workflowManager.start();

            Task task2 = new Task(new TaskId(), taskType);
            Task task1 = new Task(new TaskId(), taskType, Lists.newArrayList(task2));
            RunId runId = workflowManager.submitTask(task1);

            Assert.assertTrue(timing.acquireSemaphore(executionLatch, 1));

            workflowManager.cancelRun(runId);
            continueLatch.countDown();

            Assert.assertFalse(executionLatch.tryAcquire(1, 5, TimeUnit.SECONDS));  // no more executions should occur
        }
        finally
        {
            closeWorkflow(workflowManager);
        }
    }

    @Test
    public void testSingleClientSimple() throws Exception
    {
        TestTaskExecutor taskExecutor = new TestTaskExecutor(6);
        WorkflowManager workflowManager = WorkflowManagerBuilder.builder()
            .addingTaskExecutor(taskExecutor, 10, new TaskType("test", "1", true))
            .withCurator(curator, "test", "1")
            .build();
        try
        {
            workflowManager.start();

            WorkflowManagerStateSampler sampler = new WorkflowManagerStateSampler(workflowManager.getAdmin(), 10, Duration.ofMillis(100));
            sampler.start();

            String json = Resources.toString(Resources.getResource("tasks.json"), Charset.defaultCharset());
            JsonSerializerMapper jsonSerializerMapper = new JsonSerializerMapper();
            Task task = jsonSerializerMapper.get(jsonSerializerMapper.getMapper().readTree(json), Task.class);
            workflowManager.submitTask(task);

            Assert.assertTrue(timing.awaitLatch(taskExecutor.getLatch()));

            List<Set<TaskId>> sets = taskExecutor.getChecker().getSets();
            List<Set<TaskId>> expectedSets = Arrays.<Set<TaskId>>asList
                (
                    Sets.newHashSet(new TaskId("task1"), new TaskId("task2")),
                    Sets.newHashSet(new TaskId("task3"), new TaskId("task4"), new TaskId("task5")),
                    Sets.newHashSet(new TaskId("task6"))
                );
            Assert.assertEquals(sets, expectedSets);

            taskExecutor.getChecker().assertNoDuplicates();

            sampler.close();
            log.info("Samples {}", sampler.getSamples());
        }
        finally
        {
            closeWorkflow(workflowManager);
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

            Assert.assertTrue(timing.awaitLatch(taskExecutor.getLatch()));

            List<Set<TaskId>> sets = taskExecutor.getChecker().getSets();
            List<Set<TaskId>> expectedSets = Arrays.<Set<TaskId>>asList
                (
                    Sets.newHashSet(new TaskId("task1"), new TaskId("task2")),
                    Sets.newHashSet(new TaskId("task3"), new TaskId("task4"), new TaskId("task5")),
                    Sets.newHashSet(new TaskId("task6"))
                );
            Assert.assertEquals(sets, expectedSets);

            taskExecutor.getChecker().assertNoDuplicates();
        }
        finally
        {
            workflowManagers.forEach(CloseableUtils::closeQuietly);
        }
    }

    @Test
    public void testNoData() throws Exception
    {
        WorkflowManager workflowManager = WorkflowManagerBuilder.builder()
            .addingTaskExecutor(new TestTaskExecutor(1), 10, new TaskType("test", "1", true))
            .withCurator(curator, "test", "1")
            .build();

        Optional<TaskExecutionResult> taskData = workflowManager.getTaskExecutionResult(new RunId(), new TaskId());
        Assert.assertFalse(taskData.isPresent());
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
        WorkflowManager workflowManager = WorkflowManagerBuilder.builder()
            .addingTaskExecutor(taskExecutor, 10, taskType)
            .withCurator(curator, "test", "1")
            .build();
        try
        {
            workflowManager.start();

            TaskId taskId = new TaskId();
            RunId runId = workflowManager.submitTask(new Task(taskId, taskType));

            Assert.assertTrue(timing.awaitLatch(latch));
            timing.sleepABit();

            Optional<TaskExecutionResult> taskData = workflowManager.getTaskExecutionResult(runId, taskId);
            Assert.assertTrue(taskData.isPresent());
            Map<String, String> expected = Maps.newHashMap();
            expected.put("one", "1");
            expected.put("two", "2");
            Assert.assertEquals(taskData.get().getResultData(), expected);
        }
        finally
        {
            closeWorkflow(workflowManager);
        }
    }
    
    @Test
    public void testTaskProgress() throws Exception
    {
        CountDownLatch latch = new CountDownLatch(1);
        TaskExecutor taskExecutor = (w, t) -> () -> {
            w.updateTaskProgress(t.getRunId(), t.getTaskId(), 50);
            try
            {
                latch.await();
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
                throw new RuntimeException();
            }
            return new TaskExecutionResult(TaskExecutionStatus.SUCCESS, "");
        };
        TaskType taskType = new TaskType("test", "1", true);
        WorkflowManager workflowManager = WorkflowManagerBuilder.builder()
            .addingTaskExecutor(taskExecutor, 10, taskType)
            .withCurator(curator, "test", "1")
            .build();
        try
        {
            workflowManager.start();

            TaskId taskId = new TaskId();
            RunId runId = workflowManager.submitTask(new Task(taskId, taskType));

            timing.sleepABit();
            
            List<TaskInfo> tasks = workflowManager.getAdmin().getTaskInfo(runId);
            Assert.assertTrue(tasks.size() == 1);
            Assert.assertTrue(tasks.get(0).hasStarted());
            Assert.assertTrue(tasks.get(0).getProgress() == 50);
            latch.countDown();
        }
        finally
        {
            closeWorkflow(workflowManager);
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
            return new TaskExecutionResult(TaskExecutionStatus.SUCCESS, "test", Maps.newHashMap(), subTaskRunId);
        };
        WorkflowManager workflowManager = WorkflowManagerBuilder.builder()
            .addingTaskExecutor(taskExecutor, 10, taskType)
            .withCurator(curator, "test", "1")
            .build();
        try
        {
            workflowManager.start();
            workflowManager.submitTask(groupAParent);

            TaskId polledTaskId = tasks.poll(timing.milliseconds(), TimeUnit.MILLISECONDS);
            Assert.assertEquals(polledTaskId, groupAParent.getTaskId());
            polledTaskId = tasks.poll(timing.milliseconds(), TimeUnit.MILLISECONDS);
            Assert.assertEquals(polledTaskId, groupBTask.getTaskId());
            timing.sleepABit();
            Assert.assertNull(tasks.peek());

            latch.countDown();
            polledTaskId = tasks.poll(timing.milliseconds(), TimeUnit.MILLISECONDS);
            Assert.assertEquals(polledTaskId, groupAChild.getTaskId());
        }
        finally
        {
            closeWorkflow(workflowManager);
        }
    }

    @Test
    public void testMultiTypesExecution() throws Exception
    {
        TaskType taskType1 = new TaskType("type1", "1", true);
        TaskType taskType2 = new TaskType("type2", "1", true);
        TaskType taskType3 = new TaskType("type3", "1", true);

        TestTaskExecutor taskExecutor = new TestTaskExecutor(6);
        WorkflowManager workflowManager = WorkflowManagerBuilder.builder()
            .addingTaskExecutor(taskExecutor, 10, taskType1)
            .addingTaskExecutor(taskExecutor, 10, taskType2)
            .addingTaskExecutor(taskExecutor, 10, taskType3)
            .withCurator(curator, "test", "1")
            .build();
        try
        {
            workflowManager.start();

            String json = Resources.toString(Resources.getResource("multi-tasks.json"), Charset.defaultCharset());
            JsonSerializerMapper jsonSerializerMapper = new JsonSerializerMapper();
            Task task = jsonSerializerMapper.get(jsonSerializerMapper.getMapper().readTree(json), Task.class);
            workflowManager.submitTask(task);

            Assert.assertTrue(timing.awaitLatch(taskExecutor.getLatch()));

            List<Set<TaskId>> sets = taskExecutor.getChecker().getSets();
            List<Set<TaskId>> expectedSets = Arrays.<Set<TaskId>>asList
                (
                    Sets.newHashSet(new TaskId("task1"), new TaskId("task2")),
                    Sets.newHashSet(new TaskId("task3"), new TaskId("task4"), new TaskId("task5")),
                    Sets.newHashSet(new TaskId("task6"))
                );
            Assert.assertEquals(sets, expectedSets);

            taskExecutor.getChecker().assertNoDuplicates();
        }
        finally
        {
            closeWorkflow(workflowManager);
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

        WorkflowManager workflowManager = WorkflowManagerBuilder.builder()
            .addingTaskExecutor(taskExecutor1, 10, taskType1)
            .addingTaskExecutor(taskExecutor2, 10, taskType2)
            .addingTaskExecutor(taskExecutor3, 10, taskType3)
            .withCurator(curator, "test", "1")
            .build();
        try
        {
            workflowManager.start();

            String json = Resources.toString(Resources.getResource("multi-tasks.json"), Charset.defaultCharset());
            JsonSerializerMapper jsonSerializerMapper = new JsonSerializerMapper();
            Task task = jsonSerializerMapper.get(jsonSerializerMapper.getMapper().readTree(json), Task.class);
            workflowManager.submitTask(task);

            Set<TaskId> set1 = Sets.newHashSet(queue1.poll(timing.milliseconds(), TimeUnit.MILLISECONDS), queue1.poll(timing.milliseconds(), TimeUnit.MILLISECONDS));
            Set<TaskId> set2 = Sets.newHashSet(queue2.poll(timing.milliseconds(), TimeUnit.MILLISECONDS), queue2.poll(timing.milliseconds(), TimeUnit.MILLISECONDS));
            Set<TaskId> set3 = Sets.newHashSet(queue3.poll(timing.milliseconds(), TimeUnit.MILLISECONDS), queue3.poll(timing.milliseconds(), TimeUnit.MILLISECONDS));

            Assert.assertEquals(set1, Sets.newHashSet(new TaskId("task1"), new TaskId("task2")));
            Assert.assertEquals(set2, Sets.newHashSet(new TaskId("task3"), new TaskId("task4")));
            Assert.assertEquals(set3, Sets.newHashSet(new TaskId("task5"), new TaskId("task6")));

            timing.sleepABit();

            Assert.assertNull(queue1.peek());
            Assert.assertNull(queue2.peek());
            Assert.assertNull(queue3.peek());
        }
        finally
        {
            closeWorkflow(workflowManager);
        }
    }

    private void closeWorkflow(WorkflowManager workflowManager) throws InterruptedException
    {
        CloseableUtils.closeQuietly(workflowManager);
        timing.sleepABit();
        ((WorkflowManagerImpl)workflowManager).debugValidateClosed();
    }
}
