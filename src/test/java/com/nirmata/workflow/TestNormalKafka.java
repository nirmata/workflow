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
import com.nirmata.workflow.models.ExecutableTask;
import com.nirmata.workflow.models.RunId;
import com.nirmata.workflow.models.Task;
import com.nirmata.workflow.models.TaskExecutionResult;
import com.nirmata.workflow.models.TaskId;
import com.nirmata.workflow.models.TaskType;
import com.nirmata.workflow.admin.RunInfo;
import com.nirmata.workflow.admin.StandardAutoCleaner;
import com.nirmata.workflow.admin.TaskDetails;
import com.nirmata.workflow.admin.TaskInfo;
import com.nirmata.workflow.admin.WorkflowAdmin;
import com.nirmata.workflow.details.WorkflowManagerKafkaImpl;
import com.nirmata.workflow.executor.TaskExecution;
import com.nirmata.workflow.executor.TaskExecutionStatus;
import com.nirmata.workflow.executor.TaskExecutor;
import com.nirmata.workflow.serialization.JsonSerializerMapper;

import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class TestNormalKafka extends BaseForTests {
    protected Properties kafkaProps = new Properties();
    protected final Timing timing = new Timing();

    private final Logger log = LoggerFactory.getLogger(getClass());

    @BeforeMethod
    protected void initTest(Method method) throws Exception {
        if (!runKafkaTests) {
            log.warn("Skipping test {}, kafka disabled", method.getName());
            throw new SkipException("Skipping test, kafka disabled");
        }
        initTopicOffsets(new String[] { "test", "type1", "type2", "type3" });
        cleanDB();
        log.info("====Starting test {}====", method.getName());
    }

    @AfterMethod
    protected void afterTest(Method method) throws Exception {
        log.info("====Done test {}====", method.getName());
    }

    @Test(enabled = true)
    public void testFailedStop() throws Exception {
        TestTaskExecutor taskExecutor = new TestTaskExecutor(2) {
            @Override
            public TaskExecution newTaskExecution(WorkflowManager workflowManager, ExecutableTask task) {
                if (task.getTaskId().getId().equals("task3")) {
                    return () -> new TaskExecutionResult(TaskExecutionStatus.FAILED_STOP, "stop");
                }
                return super.newTaskExecution(workflowManager, task);
            }
        };
        TaskType taskType = new TaskType("test", "1", true);
        WorkflowManager workflowManager = createWorkflowKafkaBuilder()
                .addingTaskExecutor(taskExecutor, 10, taskType)
                .build();
        try {
            workflowManager.start();

            Task task4 = new Task(new TaskId("task4"), taskType);
            Task task3 = new Task(new TaskId("task3"), taskType, Lists.newArrayList(task4));
            Task task2 = new Task(new TaskId("task2"), taskType, Lists.newArrayList(task3));
            Task task1 = new Task(new TaskId("task1"), taskType, Lists.newArrayList(task2));
            RunId runId = workflowManager.submitTask(task1);

            Assert.assertTrue(timing.awaitLatch(taskExecutor.getLatch()));
            // timing.sleepABit(); // make sure other tasks are not started
            sleepForRunCompletion();

            RunInfo runInfo = workflowManager.getAdmin().getRunInfo(runId);
            Assert.assertTrue(!useMongo || runInfo.isComplete());

            List<Set<TaskId>> sets = taskExecutor.getChecker().getSets();
            List<Set<TaskId>> expectedSets = Arrays.<Set<TaskId>>asList(
                    Sets.newHashSet(new TaskId("task1")),
                    Sets.newHashSet(new TaskId("task2")));
            Assert.assertEquals(sets, expectedSets);
        } finally {
            closeWorkflow(workflowManager);
        }
    }

    // Running this test last, because, autocleaner
    // might clear runs unexpectedly especially if tests
    // are run in parallel
    @Test(enabled = true)
    public void zzTestAutoCleanRun() throws Exception {

        TaskExecutor taskExecutor = (w, t) -> () -> new TaskExecutionResult(TaskExecutionStatus.SUCCESS, "");
        TaskType taskType = new TaskType("test", "1", true);
        WorkflowManager workflowManager = createWorkflowKafkaBuilder()
                .addingTaskExecutor(taskExecutor, 10, taskType)
                .withAutoCleaner(new StandardAutoCleaner(Duration.ofMillis(1)), Duration.ofMillis(1))
                .build();
        try {
            workflowManager.start();

            Task task2 = new Task(new TaskId(), taskType);
            Task task1 = new Task(new TaskId(), taskType, Lists.newArrayList(task2));
            RunId rid = workflowManager.submitTask(task1);

            Thread.sleep(3000);

            Assert.assertNull(workflowManager.getAdmin().getRunInfo(rid)); // asserts that the cleaner ran
        } finally {
            closeWorkflow(workflowManager);
        }
    }

    @Test(enabled = true)
    public void testCanceling() throws Exception {
        Semaphore executionLatch = new Semaphore(0);
        CountDownLatch continueLatch = new CountDownLatch(1);

        TaskExecutor taskExecutor = (w, t) -> () -> {
            executionLatch.release();
            try {
                continueLatch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return new TaskExecutionResult(TaskExecutionStatus.SUCCESS, "");
        };
        TaskType taskType = new TaskType("test", "1", true);
        WorkflowManager workflowManager = createWorkflowKafkaBuilder()
                .addingTaskExecutor(taskExecutor, 10, taskType)
                .build();
        try {
            workflowManager.start();

            Task task2 = new Task(new TaskId(), taskType);
            Task task1 = new Task(new TaskId(), taskType, Lists.newArrayList(task2));
            RunId runId = workflowManager.submitTask(task1);

            Assert.assertTrue(timing.acquireSemaphore(executionLatch, 1));

            workflowManager.cancelRun(runId);
            continueLatch.countDown();

            Assert.assertFalse(executionLatch.tryAcquire(1, 5, TimeUnit.SECONDS)); // no more executions should occur
        } finally {
            closeWorkflow(workflowManager);
        }
    }

    @Test(enabled = true)
    public void testSingleClientSimple() throws Exception {
        TestTaskExecutor taskExecutor = new TestTaskExecutor(6);
        WorkflowManager workflowManager = createWorkflowKafkaBuilder()
                .addingTaskExecutor(taskExecutor, 10, new TaskType("test", "1", true)).build();
        try {

            workflowManager.start();
            WorkflowManagerStateSampler sampler = new WorkflowManagerStateSampler(workflowManager.getAdmin(), 10,
                    Duration.ofMillis(100));
            sampler.start();

            timing.sleepABit();

            String json = Resources.toString(Resources.getResource("tasks.json"), Charset.defaultCharset());
            JsonSerializerMapper jsonSerializerMapper = new JsonSerializerMapper();
            Task task = jsonSerializerMapper.get(jsonSerializerMapper.getMapper().readTree(json), Task.class);
            RunId runId = workflowManager.submitTask(task);

            taskExecutor.getLatch().await();
            sleepForRunCompletion();

            WorkflowAdmin wfAdmin = workflowManager.getAdmin();

            if (useMongo) {
                List<RunId> runIds = wfAdmin.getRunIds();
                List<RunInfo> runInfos = wfAdmin.getRunInfo();
                // Not == 1 because, sometimes there could be residual ids. Not cleaning DB
                Assert.assertTrue(runIds.size() > 0);
                Assert.assertTrue(runInfos.size() > 0);

                RunInfo runInfo = wfAdmin.getRunInfo(runId);
                Assert.assertNotNull(runInfo);
                Map<TaskId, TaskDetails> taskDetails = wfAdmin.getTaskDetails(runId);
                Assert.assertTrue(taskDetails.size() == 7);
                List<TaskInfo> taskInfo = wfAdmin.getTaskInfo(runId);
                Assert.assertTrue(taskInfo.size() == 6);
            }

            List<TaskId> flatSet = new ArrayList<TaskId>();
            for (Set<TaskId> set : taskExecutor.getChecker().getSets()) {
                flatSet.addAll(
                        set.stream().sorted((i1, i2) -> i1.getId().compareTo(i2.getId())).collect(Collectors.toList()));
            }
            List<TaskId> expectedSets = Arrays.<TaskId>asList(new TaskId("task1"), new TaskId("task2"),
                    new TaskId("task3"), new TaskId("task4"), new TaskId("task5"),
                    new TaskId("task6"));
            Assert.assertEquals(flatSet, expectedSets);

            taskExecutor.getChecker().assertNoDuplicates();

            sampler.close();
            log.info("Samples {}", sampler.getSamples());
        } finally {
            closeWorkflow(workflowManager);
        }
    }

    @Test(enabled = true)
    public void testMultiClientSimple() throws Exception {
        final int QTY = 4;

        TestTaskExecutor taskExecutor = new TestTaskExecutor(6);
        TaskType taskType = new TaskType("test", "1", true);
        List<WorkflowManager> workflowManagers = Lists.newArrayList();
        for (int i = 0; i < QTY; ++i) {
            WorkflowManager workflowManager = createWorkflowKafkaBuilder()
                    .addingTaskExecutor(taskExecutor, 10, taskType)
                    .build();
            workflowManagers.add(workflowManager);
        }
        try {
            workflowManagers.forEach(WorkflowManager::start);

            String json = Resources.toString(Resources.getResource("tasks.json"), Charset.defaultCharset());
            JsonSerializerMapper jsonSerializerMapper = new JsonSerializerMapper();
            Task task = jsonSerializerMapper.get(jsonSerializerMapper.getMapper().readTree(json), Task.class);
            workflowManagers.get(QTY - 1).submitTask(task);

            Assert.assertTrue(timing.awaitLatch(taskExecutor.getLatch()));
            sleepForRunCompletion();

            List<TaskId> flatSet = new ArrayList<TaskId>();
            for (Set<TaskId> set : taskExecutor.getChecker().getSets()) {
                flatSet.addAll(
                        set.stream().sorted((i1, i2) -> i1.getId().compareTo(i2.getId())).collect(Collectors.toList()));
            }
            List<TaskId> expectedSets = Arrays.<TaskId>asList(new TaskId("task1"), new TaskId("task2"),
                    new TaskId("task3"), new TaskId("task4"), new TaskId("task5"),
                    new TaskId("task6"));

            Assert.assertEquals(flatSet, expectedSets);

            taskExecutor.getChecker().assertNoDuplicates();
        } finally {
            workflowManagers.forEach(CloseableUtils::closeQuietly);
        }
    }

    @Test(enabled = true)
    public void testNoData() throws Exception {
        WorkflowManager workflowManager = createWorkflowKafkaBuilder()
                .addingTaskExecutor(new TestTaskExecutor(1), 10, new TaskType("test", "1", true))
                .build();

        Optional<TaskExecutionResult> taskData = workflowManager.getTaskExecutionResult(new RunId(), new TaskId());
        Assert.assertFalse(taskData.isPresent());
    }

    @Test(enabled = true)
    public void testTaskData() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        TaskExecutor taskExecutor = (w, t) -> () -> {
            latch.countDown();
            Map<String, String> resultData = Maps.newHashMap();
            resultData.put("one", "1");
            resultData.put("two", "2");
            return new TaskExecutionResult(TaskExecutionStatus.SUCCESS, "test", resultData);
        };
        TaskType taskType = new TaskType("test", "1", true);
        WorkflowManager workflowManager = createWorkflowKafkaBuilder()
                .addingTaskExecutor(taskExecutor, 10, taskType)
                .build();
        try {
            workflowManager.start();

            TaskId taskId = new TaskId();
            RunId runId = workflowManager.submitTask(new Task(taskId, taskType));

            Assert.assertTrue(timing.awaitLatch(latch));
            sleepForRunCompletion();

            Optional<TaskExecutionResult> taskData = workflowManager.getTaskExecutionResult(runId, taskId);
            Assert.assertTrue(!useMongo || taskData.isPresent());
            Map<String, String> expected = Maps.newHashMap();
            expected.put("one", "1");
            expected.put("two", "2");
            if (useMongo) {
                Assert.assertEquals(taskData.get().getResultData(), expected);
            }
        } finally {
            closeWorkflow(workflowManager);
        }
    }

    @Test(enabled = true)
    public void testTaskProgress() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        TaskExecutor taskExecutor = (w, t) -> () -> {
            w.updateTaskProgress(t.getRunId(), t.getTaskId(), 50);
            try {
                latch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException();
            }
            return new TaskExecutionResult(TaskExecutionStatus.SUCCESS, "");
        };
        TaskType taskType = new TaskType("test", "1", true);
        WorkflowManager workflowManager = createWorkflowKafkaBuilder()
                .addingTaskExecutor(taskExecutor, 10, taskType)
                .build();
        try {
            workflowManager.start();

            TaskId taskId = new TaskId();
            RunId runId = workflowManager.submitTask(new Task(taskId, taskType));

            timing.sleepABit();

            List<TaskInfo> tasks = workflowManager.getAdmin().getTaskInfo(runId);
            Assert.assertTrue(!useMongo || tasks.size() == 1);
            Assert.assertTrue(!useMongo || tasks.get(0).hasStarted());
            Assert.assertTrue(!useMongo || tasks.get(0).getProgress() == 50);
            latch.countDown();
        } finally {
            sleepForRunCompletion();
            closeWorkflow(workflowManager);
        }
    }

    @Test(enabled = true)
    public void testSubTask() throws Exception {
        TaskType taskType = new TaskType("test", "1", true);
        Task groupAChild = new Task(new TaskId(), taskType);
        Task groupAParent = new Task(new TaskId(), taskType, Lists.newArrayList(groupAChild));

        Task groupBTask = new Task(new TaskId(), taskType);

        log.debug("gaParent = {}, gaChild = {}, gb = {}", groupAParent.getTaskId(), groupAChild.getTaskId(),
                groupBTask.getTaskId());

        BlockingQueue<TaskId> tasks = Queues.newLinkedBlockingQueue();
        CountDownLatch latch = new CountDownLatch(1);
        TaskExecutor taskExecutor = (workflowManager, task) -> () -> {
            tasks.add(task.getTaskId());
            if (task.getTaskId().equals(groupBTask.getTaskId())) {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException();
                }
            }
            RunId subTaskRunId = task.getTaskId().equals(groupAParent.getTaskId())
                    ? workflowManager.submitSubTask(task.getRunId(), groupBTask)
                    : null;
            return new TaskExecutionResult(TaskExecutionStatus.SUCCESS, "test", Maps.newHashMap(), subTaskRunId);
        };
        WorkflowManager workflowManager = createWorkflowKafkaBuilder()
                .addingTaskExecutor(taskExecutor, 10, taskType)
                .build();
        try {
            workflowManager.start();
            workflowManager.submitTask(groupAParent);

            TaskId polledTaskId = tasks.poll(timing.milliseconds(), TimeUnit.MILLISECONDS);
            Assert.assertEquals(polledTaskId, groupAParent.getTaskId());
            polledTaskId = tasks.poll(timing.milliseconds(), TimeUnit.MILLISECONDS);
            Assert.assertEquals(polledTaskId, groupBTask.getTaskId());
            timing.sleepABit();
            Assert.assertNull(tasks.peek());

            latch.countDown();
            Thread.sleep(3000);
            polledTaskId = tasks.poll(timing.milliseconds(), TimeUnit.MILLISECONDS);
            Assert.assertEquals(polledTaskId, groupAChild.getTaskId());
        } finally {
            closeWorkflow(workflowManager);
        }
    }

    @Test(enabled = true)
    public void testMultiTypesExecution() throws Exception {
        TaskType taskType1 = new TaskType("type1", "1", true);
        TaskType taskType2 = new TaskType("type2", "1", true);
        TaskType taskType3 = new TaskType("type3", "1", true);

        TestTaskExecutor taskExecutor = new TestTaskExecutor(6);
        WorkflowManager workflowManager = createWorkflowKafkaBuilder()
                .addingTaskExecutor(taskExecutor, 10, taskType1)
                .addingTaskExecutor(taskExecutor, 10, taskType2)
                .addingTaskExecutor(taskExecutor, 10, taskType3)
                .build();
        try {
            workflowManager.start();

            String json = Resources.toString(Resources.getResource("multi-tasks.json"), Charset.defaultCharset());
            JsonSerializerMapper jsonSerializerMapper = new JsonSerializerMapper();
            Task task = jsonSerializerMapper.get(jsonSerializerMapper.getMapper().readTree(json), Task.class);
            workflowManager.submitTask(task);

            Assert.assertTrue(timing.awaitLatch(taskExecutor.getLatch()));
            sleepForRunCompletion();

            List<TaskId> flatSet = new ArrayList<TaskId>();
            for (Set<TaskId> set : taskExecutor.getChecker().getSets()) {
                flatSet.addAll(
                        set.stream().sorted((i1, i2) -> i1.getId().compareTo(i2.getId())).collect(Collectors.toList()));
            }
            List<TaskId> expectedSets = Arrays.<TaskId>asList(new TaskId("task1"), new TaskId("task2"),
                    new TaskId("task3"), new TaskId("task4"), new TaskId("task5"),
                    new TaskId("task6"));
            Assert.assertEquals(flatSet, expectedSets);

            taskExecutor.getChecker().assertNoDuplicates();
        } finally {
            closeWorkflow(workflowManager);
        }
    }

    @Test(enabled = true)
    public void testMultiTypes() throws Exception {
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

        WorkflowManager workflowManager = createWorkflowKafkaBuilder()
                .addingTaskExecutor(taskExecutor1, 10, taskType1)
                .addingTaskExecutor(taskExecutor2, 10, taskType2)
                .addingTaskExecutor(taskExecutor3, 10, taskType3)
                .build();
        try {
            workflowManager.start();

            String json = Resources.toString(Resources.getResource("multi-tasks.json"), Charset.defaultCharset());
            JsonSerializerMapper jsonSerializerMapper = new JsonSerializerMapper();
            Task task = jsonSerializerMapper.get(jsonSerializerMapper.getMapper().readTree(json), Task.class);
            workflowManager.submitTask(task);

            sleepForRunCompletion();

            Set<TaskId> set1 = Sets.newHashSet(queue1.poll(timing.milliseconds(), TimeUnit.MILLISECONDS),
                    queue1.poll(timing.milliseconds(), TimeUnit.MILLISECONDS));
            Set<TaskId> set2 = Sets.newHashSet(queue2.poll(timing.milliseconds(), TimeUnit.MILLISECONDS),
                    queue2.poll(timing.milliseconds(), TimeUnit.MILLISECONDS));
            Set<TaskId> set3 = Sets.newHashSet(queue3.poll(timing.milliseconds(), TimeUnit.MILLISECONDS),
                    queue3.poll(timing.milliseconds(), TimeUnit.MILLISECONDS));

            Assert.assertEquals(set1, Sets.newHashSet(new TaskId("task1"), new TaskId("task2")));
            Assert.assertEquals(set2, Sets.newHashSet(new TaskId("task3"), new TaskId("task4")));
            Assert.assertEquals(set3, Sets.newHashSet(new TaskId("task5"), new TaskId("task6")));

            timing.sleepABit();

            Assert.assertNull(queue1.peek());
            Assert.assertNull(queue2.peek());
            Assert.assertNull(queue3.peek());
        } finally {
            closeWorkflow(workflowManager);
        }
    }

    private void closeWorkflow(WorkflowManager workflowManager) throws InterruptedException {
        CloseableUtils.closeQuietly(workflowManager);
        timing.sleepABit();
        ((WorkflowManagerKafkaImpl) workflowManager).debugValidateClosed();
    }

}
