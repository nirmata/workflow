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

import com.nirmata.workflow.details.WorkflowManagerImpl;
import com.nirmata.workflow.models.TaskType;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestLoadZookeeper extends TestLoadBase {
    protected CuratorFramework curator;
    protected final Timing timing = new Timing();
    private final Logger log = LoggerFactory.getLogger(getClass());
    private static final String ZKP_NS = "testzkpns";
    private static final String ZKP_NS_VER = "v1";

    @BeforeMethod
    public void setup() throws Exception {

        curator = CuratorFrameworkFactory.newClient("localhost:2181", new RetryOneTime(1));
        curator.start();
        try {
            curator.delete().deletingChildrenIfNeeded().forPath("/" + ZKP_NS + "-" + ZKP_NS_VER);
        } catch (KeeperException.NoNodeException e) {
            log.warn("No nodes to cleanup mostly. Proceeding with test");
        }
    }

    @AfterMethod
    public void teardown() throws Exception {
        curator.delete().deletingChildrenIfNeeded().forPath("/" + ZKP_NS + "-" + ZKP_NS_VER);
        CloseableUtils.closeQuietly(curator);
    }

    @Test(enabled = false)
    public void testLoadZkp1() throws Exception {
        TestTaskExecutor taskExecutor = new TestTaskExecutor(getTest1Tasks(), false, getTest1Delay());
        WorkflowManager workflowManager = WorkflowManagerBuilder.builder()
                .addingTaskExecutor(taskExecutor, 10, new TaskType("test", "1", true))
                .withCurator(curator, ZKP_NS, ZKP_NS_VER)
                .build();
        try {

            super.testLoad1(workflowManager, taskExecutor);
        } catch (Exception e) {
            log.error("Unexpected exception: ", e);
        } finally {
            closeWorkflow(workflowManager);
        }
    }

    protected void closeWorkflow(WorkflowManager workflowManager) throws InterruptedException {
        Thread.sleep(getTest1Tasks() * 10);
        CloseableUtils.closeQuietly(workflowManager);
        timing.sleepABit();
        ((WorkflowManagerImpl) workflowManager).debugValidateClosed();
    }
}
