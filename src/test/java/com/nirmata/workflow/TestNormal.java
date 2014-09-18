package com.nirmata.workflow;

import com.google.common.collect.Sets;
import com.nirmata.workflow.models.ScheduleId;
import com.nirmata.workflow.models.TaskId;
import com.nirmata.workflow.spi.StorageBridge;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

public class TestNormal extends BaseClassForTests
{
    private CuratorFramework curator;

    @BeforeMethod
    public void setup() throws Exception
    {
        super.setup();

        curator = CuratorFrameworkFactory.builder().connectString(server.getConnectString()).namespace("test").retryPolicy(new RetryOneTime(1)).build();
        curator.start();
    }

    @AfterMethod
    public void teardown() throws Exception
    {
        CloseableUtils.closeQuietly(curator);

        super.teardown();
    }

    @Test
    public void testNormal_1x() throws Exception
    {
        StorageBridge storageBridge = new TestStorageBridge("schedule_1x.json", "tasks.json", "workflows.json");

        Timing timing = new Timing();
        WorkflowManagerConfiguration configuration = new WorkflowManagerConfigurationImpl(1000, 1000, 10, 10);
        TestTaskExecutor taskExecutor = new TestTaskExecutor(6);
        WorkflowManager workflowManager = new WorkflowManager(curator, configuration, taskExecutor, storageBridge);
        workflowManager.start();
        try
        {
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
    public void testNormal_2x() throws Exception
    {
        StorageBridge storageBridge = new TestStorageBridge("schedule_2x.json", "tasks.json", "workflows.json");

        Timing timing = new Timing();
        WorkflowManagerConfiguration configuration = new WorkflowManagerConfigurationImpl(1000, 1000, 10, 10);
        TestTaskExecutor taskExecutor = new TestTaskExecutor(6);
        final CountDownLatch scheduleLatch = new CountDownLatch(2);
        WorkflowManager workflowManager = new WorkflowManager(curator, configuration, taskExecutor, storageBridge);
        WorkflowManagerListener listener = new WorkflowManagerListener()
        {
            @Override
            public void notifyScheduleStarted(ScheduleId scheduleId)
            {
                scheduleLatch.countDown();
            }

            @Override
            public void notifyTaskExecuted(ScheduleId scheduleId, TaskId taskId)
            {

            }

            @Override
            public void notifyScheduleCompleted(ScheduleId scheduleId)
            {
            }
        };
        workflowManager.getListenable().addListener(listener);
        workflowManager.start();
        try
        {
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
            taskExecutor.reset();

            Assert.assertTrue(timing.awaitLatch(scheduleLatch));
            Assert.assertTrue(timing.awaitLatch(taskExecutor.getLatch()));

            sets = taskExecutor.getChecker().getSets();
            Assert.assertEquals(sets, expectedSets);
            taskExecutor.getChecker().assertNoDuplicates();
        }
        finally
        {
            CloseableUtils.closeQuietly(workflowManager);
        }
    }
}
