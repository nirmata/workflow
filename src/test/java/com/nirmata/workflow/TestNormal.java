package com.nirmata.workflow;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import com.nirmata.workflow.executor.TaskExecutionStatus;
import com.nirmata.workflow.executor.TaskExecutor;
import com.nirmata.workflow.models.RunId;
import com.nirmata.workflow.models.Task;
import com.nirmata.workflow.models.TaskExecutionResult;
import com.nirmata.workflow.models.TaskId;
import com.nirmata.workflow.models.TaskType;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static com.nirmata.workflow.details.JsonSerializer.fromString;
import static com.nirmata.workflow.details.JsonSerializer.getTask;

public class TestNormal
{
    private TestingServer server;
    private CuratorFramework curator;

    @BeforeMethod
    public void setup() throws Exception
    {
        server = new TestingServer();

        curator = CuratorFrameworkFactory.builder().connectString(server.getConnectString()).retryPolicy(new RetryOneTime(1)).build();
        curator.start();
    }

    @AfterMethod
    public void teardown() throws Exception
    {
        CloseableUtils.closeQuietly(curator);
        CloseableUtils.closeQuietly(server);
    }

    @Test
    public void testCanceling() throws Exception
    {
        Semaphore executionLatch = new Semaphore(0);
        CountDownLatch continueLatch = new CountDownLatch(1);
        TaskExecutor taskExecutor = executableTask -> () -> {
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

            Timing timing = new Timing();
            Assert.assertTrue(timing.acquireSemaphore(executionLatch, 1));

            workflowManager.cancelRun(runId);
            continueLatch.countDown();

            Assert.assertFalse(executionLatch.tryAcquire(1, 5, TimeUnit.SECONDS));  // no more executions should occur
        }
        finally
        {
            CloseableUtils.closeQuietly(workflowManager);
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

            String json = Resources.toString(Resources.getResource("tasks.json"), Charset.defaultCharset());
            Task task = getTask(fromString(json));
            workflowManager.submitTask(task);

            Timing timing = new Timing();
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
            CloseableUtils.closeQuietly(workflowManager);
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
            Task task = getTask(fromString(json));
            workflowManagers.get(QTY - 1).submitTask(task);

            Timing timing = new Timing();
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

        Optional<Map<String, String>> taskData = workflowManager.getTaskData(new RunId(), new TaskId());
        Assert.assertFalse(taskData.isPresent());
    }

    @Test
    public void testTaskData() throws Exception
    {
        CountDownLatch latch = new CountDownLatch(1);
        TaskExecutor taskExecutor = executableTask -> () -> {
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

            Timing timing = new Timing();
            Assert.assertTrue(timing.awaitLatch(latch));
            timing.sleepABit();

            Optional<Map<String, String>> taskData = workflowManager.getTaskData(runId, taskId);
            Assert.assertTrue(taskData.isPresent());
            Map<String, String> expected = Maps.newHashMap();
            expected.put("one", "1");
            expected.put("two", "2");
            Assert.assertEquals(taskData.get(), expected);
        }
        finally
        {
            CloseableUtils.closeQuietly(workflowManager);
        }
    }
}
