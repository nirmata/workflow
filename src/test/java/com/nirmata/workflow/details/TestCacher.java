package com.nirmata.workflow.details;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.nirmata.workflow.details.internalmodels.DenormalizedWorkflowModel;
import com.nirmata.workflow.models.ScheduleExecutionModel;
import com.nirmata.workflow.models.ScheduleId;
import com.nirmata.workflow.spi.TaskExecutionResult;
import com.nirmata.workflow.models.TaskId;
import com.nirmata.workflow.models.TaskModel;
import com.nirmata.workflow.models.TaskSets;
import com.nirmata.workflow.models.WorkflowId;
import com.nirmata.workflow.spi.Clock;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static com.nirmata.workflow.details.InternalJsonSerializer.addDenormalizedWorkflow;
import static com.nirmata.workflow.details.InternalJsonSerializer.addTaskExecutionResult;
import static com.nirmata.workflow.spi.JsonSerializer.*;

public class TestCacher extends BaseClassForTests
{
    @Test
    public void testBasic() throws Exception
    {
        Timing timing = new Timing();
        Cacher cacher = null;
        final CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();

            final TaskId taskId = new TaskId();
            final CountDownLatch latch = new CountDownLatch(2);
            CacherListener cacherListener = new CacherListener()
            {
                @Override
                public void updateAndQueueTasks(Cacher cacher, DenormalizedWorkflowModel workflow)
                {
                    latch.countDown();
                    if ( latch.getCount() > 0 )
                    {
                        Map<String, String> resultData = Maps.newHashMap();
                        TaskExecutionResult result = new TaskExecutionResult("test", resultData);
                        String json = nodeToString(addTaskExecutionResult(newNode(), result));
                        try
                        {
                            String path = ZooKeeperConstants.getCompletedTaskPath(workflow.getScheduleId(), taskId);
                            client.create().creatingParentsIfNeeded().forPath(path, json.getBytes());
                        }
                        catch ( Exception e )
                        {
                            throw new AssertionError(e);
                        }
                    }
                }
            };
            cacher = new Cacher(client, cacherListener);
            cacher.start();

            ScheduleId scheduleId = new ScheduleId();
            ScheduleExecutionModel scheduleExecution = new ScheduleExecutionModel(scheduleId, Clock.nowUtc(), Clock.nowUtc(), 1);
            List<TaskModel> tasks = Arrays.asList(new TaskModel(taskId, "test-task", "test", true));
            List<List<TaskId>> tasksSets = Lists.newArrayList();
            tasksSets.add(Arrays.asList(taskId));
            TaskSets taskSets = new TaskSets(tasksSets);
            DenormalizedWorkflowModel denormalizedWorkflow = new DenormalizedWorkflowModel(scheduleExecution, new WorkflowId(), tasks, "test", taskSets, Clock.nowUtc(), 0);
            byte[] json = toBytes(addDenormalizedWorkflow(newNode(), denormalizedWorkflow));
            client.create().creatingParentsIfNeeded().forPath(ZooKeeperConstants.getSchedulePath(scheduleId), json);

            Assert.assertTrue(timing.awaitLatch(latch));
        }
        finally
        {
            CloseableUtils.closeQuietly(cacher);
            CloseableUtils.closeQuietly(client);
        }
    }
}
