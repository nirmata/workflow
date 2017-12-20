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

import com.nirmata.workflow.admin.AutoCleaner;
import com.nirmata.workflow.admin.StandardAutoCleaner;
import com.nirmata.workflow.details.ZooKeeperConstants;
import com.nirmata.workflow.executor.TaskExecutionStatus;
import com.nirmata.workflow.executor.TaskExecutor;
import com.nirmata.workflow.models.RunId;
import com.nirmata.workflow.models.Task;
import com.nirmata.workflow.models.TaskExecutionResult;
import com.nirmata.workflow.models.TaskId;
import com.nirmata.workflow.models.TaskType;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class TestParentNodeDeletion extends BaseForTests
{
    private static final String TASK_NAME = "TestTask";
    private static final String VERSION = "v1";
    private static final TaskType TASK_TYPE = new TaskType(TASK_NAME, VERSION, true);
    private static final int CONCURRENT_TASKS = 5;

    private static final Logger log = LoggerFactory.getLogger(TestParentNodeDeletion.class);

    @Test
    public void testParentNodeDeletion() throws Exception
    {
        final String namespace = "test";
        final String workflowPath = "/" + namespace + "-" + VERSION;

        try ( CuratorFramework curator = newCurator() )
        {
            WorkflowManager workflowManager = buildWorkflow(curator, namespace);
            RunId runId = submitTask(workflowManager);
            assertTask(workflowManager, runId);

            String runPath = ZKPaths.makePath(workflowPath, ZooKeeperConstants.getRunParentPath());
            String startedPath = ZKPaths.makePath(workflowPath, ZooKeeperConstants.getStartedTasksParentPath());
            String completedPath = ZKPaths.makePath(workflowPath, ZooKeeperConstants.getCompletedTaskParentPath());
            String queuePath = ZKPaths.makePath(workflowPath, ZooKeeperConstants.getQueuePathBase());

            deletePath(curator, runPath);
            deletePath(curator, startedPath);
            deletePath(curator, completedPath);

            timing.sleepABit();

            runId = submitTask(workflowManager);
            assertTask(workflowManager, runId);
        }
    }

    private void assertTask(WorkflowManager workflowManager, RunId runId) throws Exception
    {
        Assert.assertTrue(workflowManager.getAdmin().getRunIds().contains(runId));
        long start = System.nanoTime();
        for(;;)
        {
            long elapsed = System.nanoTime() - start;
            if ( TimeUnit.NANOSECONDS.toMillis(elapsed) > timing.forWaiting().milliseconds() )
            {
                Assert.fail("Task did not execute within timeout");
            }
            Assert.assertTrue(workflowManager.getAdmin().getRunIds().contains(runId));
            if ( workflowManager.getAdmin().getRunInfo(runId).isComplete() )
            {
                break;
            }
            timing.sleepABit();
        }
    }

    private void deletePath(CuratorFramework curator, String path) throws Exception
    {
        try
        {
            curator.delete().deletingChildrenIfNeeded().forPath(path);
        }
        catch ( KeeperException ignore )
        {
            // ignore
        }
    }

    private CuratorFramework newCurator()
    {
        final CuratorFramework curator = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new ExponentialBackoffRetry(1000, 10));
        curator.start();
        return curator;
    }

    private WorkflowManager buildWorkflow(CuratorFramework curator, String namespace)
    {
        Duration runPeriod = Duration.ofSeconds(5);
        AutoCleaner cleaner = new StandardAutoCleaner(Duration.ofSeconds(5));

        final WorkflowManagerBuilder workflowManagerBuilder = WorkflowManagerBuilder.builder().withCurator(curator, namespace, VERSION).withAutoCleaner(cleaner, runPeriod);

        final TaskExecutor taskExecutor = (workflowManager, executableTask) -> () -> {
            final String runId = executableTask.getRunId().getId();
            final String taskId = executableTask.getTaskId().getId();
            log.debug("execute task {} - {}", runId, taskId);

            return new TaskExecutionResult(TaskExecutionStatus.SUCCESS, "");
        };

        workflowManagerBuilder.addingTaskExecutor(taskExecutor, CONCURRENT_TASKS, TASK_TYPE);

        final WorkflowManager workflowManager = workflowManagerBuilder.build();
        workflowManager.start();

        return workflowManager;
    }

    private RunId submitTask(WorkflowManager workflowManager)
    {
        TaskId taskId = new TaskId();
        Task task = new Task(taskId, TASK_TYPE);
        RunId runId = workflowManager.submitTask(task);
        log.debug("submit task done - runId {}", runId.getId());
        return runId;
    }
}
