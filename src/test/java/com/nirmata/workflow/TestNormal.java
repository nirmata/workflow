package com.nirmata.workflow;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import com.nirmata.workflow.models.Task;
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
import java.util.Set;

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
}
