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

import java.util.Collections;

import com.nirmata.workflow.details.KafkaHelper;
import com.nirmata.workflow.models.RunId;
import com.nirmata.workflow.models.TaskType;
import com.nirmata.workflow.storage.StorageManager;
import com.nirmata.workflow.storage.StorageManagerMongoImpl;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

public abstract class BaseForTests {
    protected static String KAFKA_ADDR = "localhost:9092";
    protected static String MONGO_URI = "mongodb://localhost:27017";
    protected static String NAMESPACE = "testns";
    protected static String NAMESPACE_VER = "v1";
    protected static boolean runKafkaTests = true;
    protected static boolean useMongo = false;

    private final Logger log = LoggerFactory.getLogger(getClass());
    protected TestingServer server;
    protected CuratorFramework curator;
    protected final Timing timing = new Timing();

    static {
        if (System.getProperty("kafka.test.enable", "true").equalsIgnoreCase("false")) {
            runKafkaTests = false;
        }
        if (System.getProperty("mongo.test.enable", "true").equalsIgnoreCase("false")) {
            useMongo = false;
        }
    }

    @BeforeMethod
    public void setup() throws Exception {
        server = new TestingServer();

        curator = CuratorFrameworkFactory.builder().connectString(server.getConnectString())
                .retryPolicy(new RetryOneTime(1)).build();
        curator.start();
    }

    @AfterMethod
    public void teardown() throws Exception {
        CloseableUtils.closeQuietly(curator);
        CloseableUtils.closeQuietly(server);
    }

    protected WorkflowManagerKafkaBuilder createWorkflowKafkaBuilder() {
        try {
            WorkflowManagerKafkaBuilder builder = WorkflowManagerKafkaBuilder.builder()
                    .withKafka(KAFKA_ADDR, NAMESPACE, NAMESPACE_VER);
            if (useMongo) {
                builder = builder.withMongo(MONGO_URI);
            }
            return builder;
        } catch (Exception e) {
            log.error("Could not create workflow manager with kafka and Mongo", e);
            throw e;
        }
    }

    // Give Kafka some time to record final run completion
    // Else, consumer offsets are not recorded, and next tests
    // fail (or all queues need to be cleared)
    protected void sleepForRunCompletion() {
        try {
            Thread.sleep(250);
        } catch (Exception e) {
            log.warn("Sleep interrupted", e);
        }
    }

    /**
     * Since tests run one after the other, and can die in between, just ensure that
     * the workflow as well as task type topic queue offsets are reset to the end
     */
    protected void initTopicOffsets(String[] taskTypeIds) {
        KafkaHelper kh = new KafkaHelper(KAFKA_ADDR, NAMESPACE, NAMESPACE_VER);
        log.debug("Init topic offsets");
        kh.createWorkflowTopicIfNeeded();
        // Clean workflow topics if topic exists
        Consumer<String, byte[]> wflowConsumer = new KafkaConsumer<String, byte[]>(
                kh.getConsumerProps(kh.getWorkflowConsumerGroup()));
        wflowConsumer.subscribe(Collections.singletonList(kh.getWorkflowTopic()));
        while (true) {
            ConsumerRecords<String, byte[]> recs = wflowConsumer.poll(100);
            if (recs.count() == 0) {
                wflowConsumer.close();
                break;
            }
        }

        // Clean task topics
        for (String taskTypeId : taskTypeIds) {
            TaskType ttype = new TaskType(taskTypeId, "", true);
            kh.createTaskTopicIfNeeded(ttype);
            Consumer<String, byte[]> consumer = new KafkaConsumer<String, byte[]>(
                    kh.getConsumerProps(kh.getTaskWorkerConsumerGroup(ttype)));
            consumer.subscribe(Collections.singletonList(kh.getTaskExecTopic(ttype)));
            while (true) {
                ConsumerRecords<String, byte[]> recs = consumer.poll(100);
                if (recs.count() == 0) {
                    consumer.close();
                    break;
                }
            }
        }
        log.debug("Init topic offsets done");
    }

    protected void cleanDB() {
        if (!useMongo) {
            return;
        }
        KafkaHelper kh = new KafkaHelper(KAFKA_ADDR, NAMESPACE, NAMESPACE_VER);

        // Clean DB
        StorageManager mgr = new StorageManagerMongoImpl(MONGO_URI, kh.getNamespace(), kh.getVersion());
        for (String runId : mgr.getRunIds()) {
            mgr.clean(new RunId(runId));
        }

    }

}
