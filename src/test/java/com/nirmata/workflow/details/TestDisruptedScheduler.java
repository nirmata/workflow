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
package com.nirmata.workflow.details;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import com.nirmata.workflow.TestTaskExecutor;
import com.nirmata.workflow.WorkflowManager;
import com.nirmata.workflow.WorkflowManagerBuilder;
import com.nirmata.workflow.models.ExecutableTask;
import com.nirmata.workflow.models.Task;
import com.nirmata.workflow.models.TaskId;
import com.nirmata.workflow.models.TaskType;
import com.nirmata.workflow.serialization.JsonSerializerMapper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingCluster;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;

public class TestDisruptedScheduler
{
    private static final TaskType taskType = new TaskType("test", "1", true);
    private static final Timing timing = new Timing(1, 2);

    private TestingCluster cluster;
    private List<Client> clients;
    private Set<TaskId> executedTasks;
    private CountDownLatch executedTasksLatch;

    @BeforeMethod
    public void setup() throws Exception
    {
        cluster = new TestingCluster(3);
        cluster.start();

        clients = Lists.newArrayList();
        executedTasks = Sets.newConcurrentHashSet();
        executedTasksLatch = new CountDownLatch(6);
    }

    @AfterMethod
    public void teardown() throws Exception
    {
        clients.forEach(CloseableUtils::closeQuietly);
        clients = null;
        CloseableUtils.closeQuietly(cluster);
    }

    private static class Client implements Closeable
    {
        final CuratorFramework curator;
        final WorkflowManager workflowManager;

        private Client(int id, TestingCluster cluster, Set<TaskId> executedTasks, CountDownLatch executedTasksLatch)
        {
            curator = CuratorFrameworkFactory.builder().connectString(cluster.getConnectString()).retryPolicy(new ExponentialBackoffRetry(10, 3)).build();
            curator.start();

            TestTaskExecutor taskExecutor = new TestTaskExecutor(6) {
                @Override
                protected void doRun(ExecutableTask task) throws InterruptedException
                {
                    executedTasks.add(task.getTaskId());
                    timing.forWaiting().sleepABit();
                    executedTasksLatch.countDown();
                }
            };

            workflowManager = WorkflowManagerBuilder.builder()
                .addingTaskExecutor(taskExecutor, 10, taskType)
                .withCurator(curator, "test", "1")
                .withInstanceName("i-" + id)
                .build();
            workflowManager.start();
        }

        @Override
        public void close() throws IOException
        {
            CloseableUtils.closeQuietly(workflowManager);
            CloseableUtils.closeQuietly(curator);
        }
    }

    @Test
    public void testDisruptedScheduler() throws Exception
    {
        final int QTY = 3;
        IntStream.range(0, QTY).forEach(i -> clients.add(new Client(i, cluster, executedTasks, executedTasksLatch)));

        Optional<Client> clientOptional = Optional.empty();
        for ( int i = 0; !clientOptional.isPresent() && (i < 3); ++i )
        {
            timing.sleepABit();
            clientOptional = clients
                .stream()
                .filter(client -> ((WorkflowManagerImpl)client.workflowManager).getSchedulerSelector().getLeaderSelector().hasLeadership())
                .findFirst();
        }
        Assert.assertTrue(clientOptional.isPresent());
        Client scheduler = clientOptional.get();
        Client nonScheduler = clients.get(0).equals(scheduler) ? clients.get(1) : clients.get(0);

        String json = Resources.toString(Resources.getResource("tasks.json"), Charset.defaultCharset());
        JsonSerializerMapper jsonSerializerMapper = new JsonSerializerMapper();
        Task task = jsonSerializerMapper.get(jsonSerializerMapper.getMapper().readTree(json), Task.class);
        nonScheduler.workflowManager.submitTask(task);  // additional test - submit to the non-scheduler

        while ( executedTasks.size() == 0 ) // wait until some tasks have started
        {
            Thread.sleep(100);
        }

        CountDownLatch latch = new CountDownLatch(1);
        ((WorkflowManagerImpl)scheduler.workflowManager).getSchedulerSelector().debugLatch.set(latch);
        ((WorkflowManagerImpl)scheduler.workflowManager).getSchedulerSelector().getLeaderSelector().interruptLeadership();  // interrupt the scheduler wherever it is
        Assert.assertTrue(executedTasks.size() < 6);
        Assert.assertTrue(timing.awaitLatch(latch)); // wait for the scheduler method to exit

        Assert.assertTrue(timing.awaitLatch(executedTasksLatch));   // all tasks should complete
    }
}
