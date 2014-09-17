package com.nirmata.workflow;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;
import com.nirmata.workflow.models.ScheduleExecutionModel;
import com.nirmata.workflow.models.ScheduleId;
import com.nirmata.workflow.models.ScheduleModel;
import com.nirmata.workflow.models.TaskModel;
import com.nirmata.workflow.models.WorkflowModel;
import com.nirmata.workflow.spi.StorageBridge;
import com.nirmata.workflow.spi.TaskExecution;
import com.nirmata.workflow.spi.TaskExecutionResult;
import com.nirmata.workflow.spi.TaskExecutor;
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
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static com.nirmata.workflow.spi.JsonSerializer.*;

public class TestNormal extends BaseClassForTests
{
    private StorageBridge storageBridge;
    private CuratorFramework curator;

    @BeforeMethod
    public void setup() throws Exception
    {
        super.setup();

        final Map<ScheduleId, ScheduleExecutionModel> scheduleExecutions = Maps.newHashMap();
        final List<ScheduleModel> schedules = getSchedules(fromString(Resources.toString(Resources.getResource("schedules.json"), Charset.defaultCharset())));
        final List<TaskModel> tasks = getTasks(fromString(Resources.toString(Resources.getResource("tasks.json"), Charset.defaultCharset())));
        final List<WorkflowModel> workflows = getWorkflows(fromString(Resources.toString(Resources.getResource("workflows.json"), Charset.defaultCharset())));
        storageBridge = new StorageBridge()
        {
            @Override
            public List<ScheduleModel> getScheduleModels()
            {
                return schedules;
            }

            @Override
            public List<WorkflowModel> getWorkflowModels()
            {
                return workflows;
            }

            @Override
            public List<TaskModel> getTaskModels()
            {
                return tasks;
            }

            @Override
            public List<ScheduleExecutionModel> getScheduleExecutions()
            {
                return Lists.newArrayList(scheduleExecutions.values());
            }

            @Override
            public void updateScheduleExecution(ScheduleExecutionModel scheduleExecution)
            {
                scheduleExecutions.put(scheduleExecution.getScheduleId(), scheduleExecution);
            }
        };

        curator = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        curator.start();
    }

    @AfterMethod
    public void teardown() throws Exception
    {
        CloseableUtils.closeQuietly(curator);

        super.teardown();
    }

    @Test
    public void testNormal() throws Exception
    {
        Timing timing = new Timing();
        WorkflowManagerConfiguration configuration = new WorkflowManagerConfigurationImpl(1, 1, 10, 10);
        final CountDownLatch latch = new CountDownLatch(6);
        final ConcurrentTaskChecker checker = new ConcurrentTaskChecker();
        TaskExecutor taskExecutor = new TaskExecutor()
        {
            @Override
            public TaskExecution newTaskExecution(final TaskModel task)
            {
                return new TaskExecution()
                {
                    @Override
                    public TaskExecutionResult execute()
                    {
                        try
                        {
                            checker.add(task.getTaskId());
                            Thread.sleep(3000);
                        }
                        catch ( InterruptedException e )
                        {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        }
                        finally
                        {
                            checker.decrement();
                            latch.countDown();
                        }
                        return new TaskExecutionResult("hey", Maps.<String, String>newHashMap());
                    }
                };
            }
        };
        WorkflowManager workflowManager = new WorkflowManager(curator, configuration, taskExecutor, storageBridge);
        workflowManager.start();
        try
        {
            Assert.assertTrue(timing.awaitLatch(latch));
        }
        finally
        {
            CloseableUtils.closeQuietly(workflowManager);
        }

        System.out.println(checker.getSets());
        System.out.println(checker.getAll());
    }
}
