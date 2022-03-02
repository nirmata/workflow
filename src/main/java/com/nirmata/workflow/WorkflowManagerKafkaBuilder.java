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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import com.nirmata.workflow.admin.AutoCleaner;
import com.nirmata.workflow.details.AutoCleanerHolder;
import com.nirmata.workflow.details.KafkaHelper;
import com.nirmata.workflow.details.TaskExecutorSpec;
import com.nirmata.workflow.details.WorkflowManagerKafkaImpl;
import com.nirmata.workflow.executor.TaskExecutor;
import com.nirmata.workflow.models.TaskType;
import com.nirmata.workflow.queue.QueueFactory;
import com.nirmata.workflow.queue.kafka.KafkaSimpleQueueFactory;
import com.nirmata.workflow.serialization.Serializer;
import com.nirmata.workflow.serialization.StandardSerializer;
import com.nirmata.workflow.storage.StorageManager;
import com.nirmata.workflow.storage.StorageManagerMongoImpl;
import com.nirmata.workflow.storage.StorageManagerNoOpImpl;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executor;

/**
 * Builds {@link WorkflowManager} instances
 */
public class WorkflowManagerKafkaBuilder {
    private QueueFactory queueFactory = new KafkaSimpleQueueFactory();
    private String instanceName;
    private KafkaHelper kafkaHelper;
    private boolean workflowWorkerEnabled = true;
    private AutoCleanerHolder autoCleanerHolder = newNullHolder();
    private Serializer serializer = new StandardSerializer();
    private Executor taskRunnerService = MoreExecutors.newDirectExecutorService();
    private StorageManager storageManager = new StorageManagerNoOpImpl();

    private final List<TaskExecutorSpec> specs = Lists.newArrayList();
    private String namespace = "";
    private String namespaceVer = "";

    /**
     * Return a new builder
     *
     * @return new builder
     */
    public static WorkflowManagerKafkaBuilder builder() {
        return new WorkflowManagerKafkaBuilder();
    }

    /**
     * <strong>required</strong><br>
     * Set the Kafka broker to use. In addition
     * to it, specify a namespace for the workflow and a version.
     * The namespace
     * and version combine to create a unique workflow. All instances using the same
     * namespace and version
     * are logically part of the same workflow.
     *
     * @param kafkaServer Kafka bootstrap servers (e.g. localhost:9092,xxx:1010)
     * @param namespace   workflow namespace
     * @param version     workflow version
     * @return this (for chaining)
     */
    public WorkflowManagerKafkaBuilder withKafka(String brokers, String namespace, String version) {
        return this.withKafka(brokers, namespace, version, 10, (short) 1);
    }

    /**
     * <strong>required</strong><br>
     * Set the Kafka broker to use. In addition
     * to it, specify a namespace for the workflow and a version.
     * The namespace
     * and version combine to create a unique workflow. All instances using the same
     * namespace and version
     * are logically part of the same workflow.
     *
     * @param kafkaServer        Kafka bootstrap servers (e.g.
     *                           localhost:9092,xxx:1010)
     * @param namespace          workflow namespace
     * @param version            workflow version
     * @param taskTypePartitions partitions for tasktype topics, at least equal to
     *                           max replicas of services with executors for a task
     *                           type
     * @param replicas           number of replicas for topics
     * @return this (for chaining)
     */
    public WorkflowManagerKafkaBuilder withKafka(String brokers, String namespace, String version,
            int taskTypeParitions, short replicas) {
        this.kafkaHelper = new KafkaHelper(brokers, namespace, version, 1, taskTypeParitions, replicas);
        this.namespace = kafkaHelper.getNamespace();
        this.namespaceVer = kafkaHelper.getVersion();

        return this;
    }

    /**
     * Set the MongoDB client to use. In addition
     * to it, specify a namespace for the workflow and a version.
     * The namespace and version combine to create a unique workflow.
     * All instances using the same namespace and version
     * are logically part of the same workflow and their data is stored in
     * the same collection. If not specified, data is not stored in Mongo
     * and admin commands also give empty response
     *
     * @param mongoUri  MongoDB connection string
     * @param namespace workflow namespace
     * @param version   workflow version
     * @return this (for chaining)
     */
    public WorkflowManagerKafkaBuilder withMongo(String connStr) {
        this.storageManager = new StorageManagerMongoImpl(connStr, namespace, namespaceVer);
        return this;
    }

    /**
     * Don't enable the workflow worker, typically used when you want to use
     * workflow manager to submit tasks only, not to execute workflow DAGs
     *
     * @return this (for chaining)
     */
    public WorkflowManagerKafkaBuilder withoutWorkflowWorker() {
        this.workflowWorkerEnabled = false;

        return this;
    }

    /**
     * <p>
     * Adds a pool of task executors for a given task type to this instance of
     * the workflow. The specified number of executors are allocated. Call this
     * method multiple times to allocate executors for the various types of tasks
     * that will be used in this workflow. You can choose to have all workflow
     * instances execute all task types or target certain task types to certain
     * instances.
     * </p>
     *
     * <p>
     * <code>qty</code> is the maximum concurrency for the given type of task for
     * this instance.
     * The logical concurrency for a given task type is the total qty of all
     * instances in the
     * workflow. e.g. if there are 3 instances in the workflow and instance A has 2
     * executors
     * for task type "a", instance B has 3 executors for task type "a" and instance
     * C has no
     * executors for task type "a", the maximum concurrency for task type "a" is 5.
     * </p>
     *
     * <p>
     * IMPORTANT: every workflow cluster must have at least one instance that has
     * task executor(s)
     * for each task type that will be submitted to the workflow. i.e workflows will
     * stall
     * if there is no executor for a given task type.
     * </p>
     *
     * @param taskExecutor the executor
     * @param qty          the number of instances for this pool
     * @param taskType     task type
     * @return this (for chaining)
     */
    public WorkflowManagerKafkaBuilder addingTaskExecutor(TaskExecutor taskExecutor, int qty, TaskType taskType) {
        specs.add(new TaskExecutorSpec(taskExecutor, qty, taskType));
        return this;
    }

    /**
     * <em>optional</em><br>
     * <p>
     * Used in reporting. This will be the value recorded as tasks are executed. Via
     * reporting, you can determine which instance has executed a given task.
     * </p>
     *
     * <p>
     * Default is: <code>InetAddress.getLocalHost().getHostName()</code>
     * </p>
     *
     * @param instanceName the name of this instance
     * @return this (for chaining)
     */
    public WorkflowManagerKafkaBuilder withInstanceName(String instanceName) {
        this.instanceName = Preconditions.checkNotNull(instanceName, "instanceName cannot be null");
        return this;
    }

    /**
     * Return a new WorkflowManager using the current builder values
     *
     * @return new WorkflowManager
     */
    public WorkflowManager build() {
        return new WorkflowManagerKafkaImpl(kafkaHelper, storageManager, workflowWorkerEnabled, queueFactory,
                instanceName, specs, autoCleanerHolder, serializer, taskRunnerService);
    }

    /**
     * Currently, only uses Kafka for queuing.
     * Send side of queue interface not implemented yet, so do not use
     *
     * @param queueFactory new queue factory
     * @return this (for chaining)
     */
    public WorkflowManagerKafkaBuilder withQueueFactory(QueueFactory queueFactory) {
        this.queueFactory = Preconditions.checkNotNull(queueFactory, "queueFactory cannot be null");
        return this;
    }

    /**
     * <em>optional</em><br>
     * Sets an auto-cleaner that will run every given period. This is used to clean
     * old runs.
     * IMPORTANT: the auto cleaner will only run on the instance that is the current
     * scheduler.
     *
     * @param autoCleaner the auto cleaner to use
     * @param runPeriod   how often to run
     * @return this (for chaining)
     */
    public WorkflowManagerKafkaBuilder withAutoCleaner(AutoCleaner autoCleaner, Duration runPeriod) {
        autoCleanerHolder = (autoCleaner == null) ? newNullHolder() : new AutoCleanerHolder(autoCleaner, runPeriod);
        return this;
    }

    /**
     * <em>optional</em><br>
     * By default, a JSON serializer is used to store data. Use this to
     * specify an alternate serializer
     *
     * @param serializer serializer to use
     * @return this (for chaining)
     */
    public WorkflowManagerKafkaBuilder withSerializer(Serializer serializer) {
        this.serializer = Preconditions.checkNotNull(serializer, "serializer cannot be null");
        return this;
    }

    /**
     * <em>optional</em><br>
     * By default, tasks are run in an internal executor service. Use this to
     * specify a custom executor service for tasks. This executor does not add any
     * async/concurrency benefit. It's purpose is to allow you to control which
     * thread executes your tasks.
     *
     * @param taskRunnerService custom executor service
     * @return this (for chaining)
     */
    public WorkflowManagerKafkaBuilder withTaskRunnerService(Executor taskRunnerService) {
        this.taskRunnerService = Preconditions.checkNotNull(taskRunnerService, "taskRunnerService cannot be null");
        return this;
    }

    private WorkflowManagerKafkaBuilder() {
        try {
            instanceName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            instanceName = "unknown";
        }
        this.kafkaHelper = new KafkaHelper("localhost:9092", "defaultns", "v1");
    }

    private AutoCleanerHolder newNullHolder() {
        return new AutoCleanerHolder(null, Duration.ofDays(Integer.MAX_VALUE));
    }
}
