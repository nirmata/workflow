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

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Preconditions;
import com.nirmata.workflow.models.TaskType;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds various kinds of Kafka related information, and utility
 * methods to form group and topic names, producer, consumer properties
 */
public class KafkaHelper {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final static String WORKFLOW_TOPIC = "workflowtopic";
    private final static String WORKFLOW_WORKER_GROUP_ID = "grp-wkflow-worker";
    private final static String TASK_WORKER_GROUP_ID = "grp-task-worker";
    private final static String TASK_TOPIC_PREFIX = "tasktopic";

    final String brokers;
    final String namespace;
    final String version;

    // Allow configuring these from outside if needed. In production
    // the needed number of partitions will already be available, or reconfigured
    // in Kafka directly from outside. Useful mainly in testing.
    // (Works only in new kafka versions e.g. 3.*)
    final int taskTypeTopicPartitions;
    final short replicationFactor;
    // Workflow topics need to be 1 (for now), to avoid issues due to rebalancing
    // consumers for multiple partitions. Once that issue is handled, even workflows
    // can scale.
    final int workflowTopicPartitions;

    public KafkaHelper(String brokers, String namespace, String version) {
        this(brokers, namespace, version, 1, 10, (short) 1);
    }

    public KafkaHelper(String brokers, String namespace, String version, int wfWorkerParts,
            int taskWorkerParts, short replicationFactor) {
        this.brokers = Preconditions.checkNotNull(brokers, "brokers should be null. e.g. host1:port1,host2:port2..");
        this.namespace = cleanForKafka(Preconditions.checkNotNull(namespace, "namespace cannot be null"));
        this.version = cleanForKafka(Preconditions.checkNotNull(version, "version cannot be null"));
        this.workflowTopicPartitions = wfWorkerParts;
        this.taskTypeTopicPartitions = taskWorkerParts;
        this.replicationFactor = replicationFactor;
    }

    // Kafka topic strings need to follow naming restrictions
    private String cleanForKafka(String str) {
        return str.replaceAll("[^a-zA-Z0-9_\\.]", "_");
    }

    public String getWorkflowConsumerGroup() {
        return namespace + "-" + version + "-" + WORKFLOW_WORKER_GROUP_ID;
    }

    public String getTaskWorkerConsumerGroup(TaskType type) {
        return namespace + "-" + version + "-" + TASK_WORKER_GROUP_ID + "-" + type.getType();
    }

    public String getWorkflowTopic() {
        return namespace + "-" + version + "-" + WORKFLOW_TOPIC;
    }

    public String getTaskExecTopic(TaskType taskType) {
        return namespace + "-" + version + "-" + TASK_TOPIC_PREFIX + "-" + taskType.getType();
    }

    public Properties getProducerProps() {
        Properties props = getBrokerProps();

        props.put(ProducerConfig.ACKS_CONFIG, "all");

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");

        return props;
    }

    public Properties getConsumerProps(String groupId) {
        Properties props = getBrokerProps();

        props.put("group.id", groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        // TODO Enhancement: when we upgrade kafka version (say, 3.1), this
        // setting is useful
        // props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
        // "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");

        return props;
    }

    private Properties getBrokerProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        return props;
    }

    public void createWorkflowTopicIfNeeded() {
        createTopic(getWorkflowTopic(), workflowTopicPartitions);
    }

    public void createTaskTopicIfNeeded(TaskType type) {
        createTopic(getTaskExecTopic(type), taskTypeTopicPartitions);
    }

    public void deleteWorkflowTopic() {
        deleteTopic(getWorkflowTopic());
    }

    public void deleteTaskTopic(TaskType type) {
        deleteTopic(getTaskExecTopic(type));
    }

    private void createTopic(String name, int partitions) {
        final NewTopic newTopic = new NewTopic(name, partitions, replicationFactor);
        try (final AdminClient adminClient = AdminClient.create(getBrokerProps())) {
            adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
        } catch (final InterruptedException | ExecutionException e) {
            // Ignore if TopicExistsException, which may be valid if topic exists
            if (!(e.getCause() instanceof TopicExistsException)) {
                log.error("Unexpected exception creating topic {}:{}", name, e);
                throw new RuntimeException(e);
            }
        }
    }

    private void deleteTopic(String name) {
        final AdminClient adminClient = AdminClient.create(getBrokerProps());
        adminClient.deleteTopics(Collections.singletonList(name));
    }

    public String getNamespace() {
        return namespace;
    }

    public String getVersion() {
        return version;
    }

}
