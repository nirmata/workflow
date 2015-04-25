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
import com.google.common.io.Resources;
import com.nirmata.workflow.admin.RunInfo;
import com.nirmata.workflow.admin.TaskDetails;
import com.nirmata.workflow.admin.TaskInfo;
import com.nirmata.workflow.details.WorkflowManagerImpl;
import com.nirmata.workflow.details.ZooKeeperConstants;
import com.nirmata.workflow.executor.TaskExecutionStatus;
import com.nirmata.workflow.executor.TaskExecutor;
import com.nirmata.workflow.models.RunId;
import com.nirmata.workflow.models.Task;
import com.nirmata.workflow.models.TaskExecutionResult;
import com.nirmata.workflow.models.TaskId;
import com.nirmata.workflow.models.TaskType;
import com.nirmata.workflow.serialization.JsonSerializerMapper;
import org.apache.curator.framework.CuratorFramework;
import org.testng.annotations.Test;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.nirmata.workflow.WorkflowAssertions.assertThat;

public class TestAdmin extends BaseForTests
{
    @Test
    public void testClean() throws Exception
    {
        CountDownLatch latch = new CountDownLatch(6);
        TaskExecutor taskExecutor = (m, t) -> () -> {
            latch.countDown();
            return new TaskExecutionResult(TaskExecutionStatus.SUCCESS, "");
        };

        try (WorkflowManager workflowManager = WorkflowManagerBuilder.builder()
                .addingTaskExecutor(taskExecutor, 10, new TaskType("test", "1", true))
                .withCurator(curator, "test", "1")
                .build())
        {

            workflowManager.start();

            String json = Resources.toString(Resources.getResource("tasks.json"), Charset.defaultCharset());
            JsonSerializerMapper jsonSerializerMapper = new JsonSerializerMapper();
            Task task = jsonSerializerMapper.get(jsonSerializerMapper.getMapper().readTree(json), Task.class);
            RunId runId = workflowManager.submitTask(task);

            assertThat(timing.awaitLatch(latch)).isTrue();

            String runParentPath = ZooKeeperConstants.getRunParentPath();
            String startedTasksParentPath = ZooKeeperConstants.getStartedTasksParentPath();
            String completedTaskParentPath = ZooKeeperConstants.getCompletedTaskParentPath();

            CuratorFramework nmCurator = ((WorkflowManagerImpl) workflowManager).getCurator();

            assertThat(nmCurator.checkExists().forPath(runParentPath).getNumChildren()).isGreaterThan(0);
            assertThat(nmCurator.checkExists().forPath(startedTasksParentPath).getNumChildren()).isGreaterThan(0);
            assertThat(nmCurator.checkExists().forPath(completedTaskParentPath).getNumChildren()).isGreaterThan(0);

            assertThat(workflowManager.getAdmin().clean(runId)).isTrue();
            timing.sleepABit();

            assertThat(nmCurator.checkExists().forPath(runParentPath).getNumChildren()).isZero();
            assertThat(nmCurator.checkExists().forPath(startedTasksParentPath).getNumChildren()).isZero();
            assertThat(nmCurator.checkExists().forPath(completedTaskParentPath).getNumChildren()).isZero();
        }
    }

    @Test
    public void testTaskInfoAndDetails() throws Exception
    {
        Map<String, String> metaData = IntStream.range(1, 5).boxed().collect(Collectors.toMap(Object::toString, Object::toString));
        TaskType taskType = new TaskType("test", "1", true);
        Task childTask = new Task(new TaskId(), taskType);
        Task task1 = new Task(new TaskId(), taskType);
        Task task2 = new Task(new TaskId(), taskType, Lists.newArrayList(childTask), metaData);
        Task root = new Task(new TaskId(), Lists.newArrayList(task1, task2));

        CountDownLatch startedLatch = new CountDownLatch(2);
        CountDownLatch waitLatch = new CountDownLatch(1);
        TaskExecutor taskExecutor = (manager, task) -> () -> {
            startedLatch.countDown();
            if ( task.getTaskId().equals(task2.getTaskId()) )
            {
                try
                {
                    waitLatch.await();
                }
                catch ( InterruptedException e )
                {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
            Map<String, String> resultData = Maps.newHashMap();
            resultData.put("taskId", task.getTaskId().getId());
            return new TaskExecutionResult(TaskExecutionStatus.SUCCESS, "", resultData);
        };

        try (WorkflowManager workflowManager = WorkflowManagerBuilder.builder()
                .addingTaskExecutor(taskExecutor, 10, taskType)
                .withCurator(curator, "test", "1")
                .build())
        {
            workflowManager.start();

            RunId runId = workflowManager.submitTask(root);

            assertThat(timing.awaitLatch(startedLatch)).isTrue();

            timing.sleepABit();

            Map<TaskId, TaskDetails> taskDetails = workflowManager.getAdmin().getTaskDetails(runId);
            assertThat(taskDetails)
                    .containsOnlyKeys(root.getTaskId(), task1.getTaskId(), task2.getTaskId(), childTask.getTaskId());

            assertThat(taskDetails.get(root.getTaskId())).matchesTask(root);
            assertThat(taskDetails.get(task1.getTaskId())).matchesTask(task1);
            assertThat(taskDetails.get(task2.getTaskId())).matchesTask(task2);
            assertThat(taskDetails.get(childTask.getTaskId())).matchesTask(childTask);

            Map<TaskId, TaskInfo> taskInfos = workflowManager.getAdmin().getTaskInfo(runId).stream().collect(Collectors.toMap(TaskInfo::getTaskId, Function.identity()));
            assertThat(taskInfos)
                    .containsOnlyKeys(task1.getTaskId(), task2.getTaskId(), childTask.getTaskId());

            assertThat(taskInfos.get(task1.getTaskId())).isComplete();
            assertThat(taskInfos.get(task2.getTaskId())).isNotComplete();
            assertThat(taskInfos.get(childTask.getTaskId())).hasNotStarted();

            waitLatch.countDown();
            timing.sleepABit();

            taskInfos = workflowManager.getAdmin().getTaskInfo(runId).stream().collect(Collectors.toMap(TaskInfo::getTaskId, Function.identity()));
            assertThat(taskInfos)
                    .containsOnlyKeys(task1.getTaskId(), task2.getTaskId(), childTask.getTaskId());

            assertThat(taskInfos.get(task1.getTaskId())).isComplete();
            assertThat(taskInfos.get(task2.getTaskId())).isComplete();
            assertThat(taskInfos.get(childTask.getTaskId())).isComplete();

            taskDetails = workflowManager.getAdmin().getTaskDetails(runId);
            assertThat(taskDetails)
                    .containsOnlyKeys(root.getTaskId(), task1.getTaskId(), task2.getTaskId(), childTask.getTaskId());

            assertThat(taskDetails.get(root.getTaskId())).matchesTask(root);
            assertThat(taskDetails.get(task1.getTaskId())).matchesTask(task1);
            assertThat(taskDetails.get(task2.getTaskId())).matchesTask(task2);
            assertThat(taskDetails.get(childTask.getTaskId())).matchesTask(childTask);
        }
    }

    @Test
    public void testRunInfo() throws Exception
    {
        TaskType taskType = new TaskType("test", "1", true);
        Task task1 = new Task(new TaskId(), taskType);
        Task task2 = new Task(new TaskId(), taskType);

        CountDownLatch startedLatch = new CountDownLatch(2);
        CountDownLatch waitLatch = new CountDownLatch(1);
        TaskExecutor taskExecutor = (manager, task) -> () -> {
            startedLatch.countDown();
            if ( task.getTaskId().equals(task2.getTaskId()) )
            {
                try
                {
                    waitLatch.await();
                }
                catch ( InterruptedException e )
                {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
            return new TaskExecutionResult(TaskExecutionStatus.SUCCESS, "");
        };
        try (WorkflowManager workflowManager = WorkflowManagerBuilder.builder()
                .addingTaskExecutor(taskExecutor, 10, taskType)
                .withCurator(curator, "test", "1")
                .build())
        {
            workflowManager.start();

            RunId runId1 = workflowManager.submitTask(task1);
            RunId runId2 = workflowManager.submitTask(task2);

            assertThat(timing.awaitLatch(startedLatch)).isTrue();

            timing.sleepABit();

            RunInfo runInfo1 = workflowManager.getAdmin().getRunInfo(runId1);
            assertThat(runInfo1.isComplete()).isTrue();

            Map<RunId, RunInfo> runs = workflowManager.getAdmin().getRunInfo().stream().collect(Collectors.toMap(RunInfo::getRunId, Function.identity()));
            assertThat(runs).containsOnlyKeys(runId1, runId2);
            assertThat(runs.get(runId1)).isComplete();
            assertThat(runs.get(runId2)).isNotComplete();

            waitLatch.countDown();
            timing.sleepABit();

            runs = workflowManager.getAdmin().getRunInfo().stream().collect(Collectors.toMap(RunInfo::getRunId, Function.identity()));
            assertThat(runs).containsOnlyKeys(runId1, runId2);
            assertThat(runs.get(runId1)).isComplete();
            assertThat(runs.get(runId2)).isComplete();
        }
    }
}
