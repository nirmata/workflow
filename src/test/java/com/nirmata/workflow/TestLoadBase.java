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

import com.google.common.io.Resources;
import com.nirmata.workflow.models.Task;
import com.nirmata.workflow.serialization.JsonSerializerMapper;

import org.apache.curator.test.Timing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.charset.Charset;
import java.time.Duration;

/**
 * Common base to ensure identical inputs for comparing load test results
 * across Zookeeper and Kafka implementations.
 * Note: Scaling in Kafka needs more partitions in task type topics depending
 * upon scale
 */
public abstract class TestLoadBase extends BaseForTests {
    protected final Timing timing = new Timing();
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final int ITERS_TEST_1 = 100;
    private final int ITERS_DELAY_MS = 1000;

    protected int getTest1Tasks() {
        return ITERS_TEST_1 * 6;
    }

    protected int getTest1Delay() {
        return ITERS_DELAY_MS;
    }

    public void testLoad1(WorkflowManager workflowManager, TestTaskExecutor taskExecutor)
            throws Exception {
        workflowManager.start();
        WorkflowManagerStateSampler sampler = new WorkflowManagerStateSampler(workflowManager.getAdmin(), 10,
                Duration.ofMillis(100));
        sampler.start();

        timing.sleepABit();

        final long startTime = System.currentTimeMillis();

        String json = Resources.toString(Resources.getResource("tasks.json"), Charset.defaultCharset());
        JsonSerializerMapper jsonSerializerMapper = new JsonSerializerMapper();

        for (int i = 0; i < ITERS_TEST_1; i++) {
            Task task = jsonSerializerMapper.get(jsonSerializerMapper.getMapper().readTree(json),
                    Task.class);
            workflowManager.submitTask(task);
        }

        taskExecutor.getLatch().await();

        log.info("===LoadTest 1===== took {} seconds", (System.currentTimeMillis() - startTime) / 1000);

        sampler.close();
        log.info("Samples {}", sampler.getSamples());

    }

    protected abstract void closeWorkflow(WorkflowManager workflowManager) throws InterruptedException;

}
