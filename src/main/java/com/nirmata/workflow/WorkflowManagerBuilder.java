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
import com.nirmata.workflow.details.TaskExecutorSpec;
import com.nirmata.workflow.details.WorkflowManagerImpl;
import com.nirmata.workflow.executor.TaskExecutor;
import com.nirmata.workflow.models.TaskType;
import com.nirmata.workflow.queue.QueueFactory;
import com.nirmata.workflow.queue.zookeeper.ZooKeeperQueueFactory;
import org.apache.curator.framework.CuratorFramework;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

/**
 * Builds {@link WorkflowManager} instances
 */
public class WorkflowManagerBuilder
{
    private QueueFactory queueFactory = new ZooKeeperQueueFactory();
    private String instanceName;
    private CuratorFramework curator;
    private final List<TaskExecutorSpec> specs = Lists.newArrayList();

    /**
     * Return a new builder
     *
     * @return new builder
     */
    public static WorkflowManagerBuilder builder()
    {
        return new WorkflowManagerBuilder();
    }

    /**
     * <strong>required</strong><br>
     * Set the Curator instance to use. In addition
     * to the Curator instance, specify a namespace for the workflow and a version. The namespace
     * and version combine to create a unique workflow. All instances using the same namespace and version
     * are logically part of the same workflow.
     *
     * @param curator Curator instance
     * @param namespace workflow namespace
     * @param version workflow version
     * @return this (for chaining)
     */
    public WorkflowManagerBuilder withCurator(CuratorFramework curator, String namespace, String version)
    {
        curator = Preconditions.checkNotNull(curator, "curator cannot be null");
        namespace = Preconditions.checkNotNull(namespace, "namespace cannot be null");
        version = Preconditions.checkNotNull(version, "version cannot be null");
        this.curator = curator.usingNamespace(namespace + "-" + version);
        return this;
    }

    /**
     * <p>
     *     Adds a pool of task executors for a given task type to this instance of
     *     the workflow. The specified number of executors are allocated. Call this
     *     method multiple times to allocate executors for the various types of tasks
     *     that will be used in this workflow. You can choose to have all workflow
     *     instances execute all task types or target certain task types to certain instances.
     * </p>
     *
     * <p>
     *     <code>qty</code> is the maximum concurrency for the given type of task for this instance.
     *     The logical concurrency for a given task type is the total qty of all instances in the
     *     workflow. e.g. if there are 3 instances in the workflow and instance A has 2 executors
     *     for task type "a", instance B has 3 executors for task type "a" and instance C has no
     *     executors for task type "a", the maximum concurrency for task type "a" is 5.
     * </p>
     *
     * <p>
     *     IMPORTANT: every workflow cluster must have at least one instance that has task executor(s)
     *     for each task type that will be submitted to the workflow. i.e workflows will stall
     *     if there is no executor for a given task type.
     * </p>
     *
     * @param taskExecutor the executor
     * @param qty the number of instances for this pool
     * @param taskType task type
     * @return this (for chaining)
     */
    public WorkflowManagerBuilder addingTaskExecutor(TaskExecutor taskExecutor, int qty, TaskType taskType)
    {
        specs.add(new TaskExecutorSpec(taskExecutor, qty, taskType));
        return this;
    }

    /**
     * <em>optional</em><br>
     * <p>
     *     Used in reporting. This will be the value recorded as tasks are executed. Via reporting,
     *     you can determine which instance has executed a given task.
     * </p>
     *
     * <p>
     *     Default is: <code>InetAddress.getLocalHost().getHostName()</code>
     * </p>
     *
     * @param instanceName the name of this instance
     * @return this (for chaining)
     */
    public WorkflowManagerBuilder withInstanceName(String instanceName)
    {
        this.instanceName = Preconditions.checkNotNull(instanceName, "instanceName cannot be null");
        return this;
    }

    /**
     * Return a new WorkflowManager using the current builder values
     *
     * @return new WorkflowManager
     */
    public WorkflowManager build()
    {
        return new WorkflowManagerImpl(curator, queueFactory, instanceName, specs);
    }

    /**
     * <em>optional</em><br>
     * Pluggable queue factory. Default uses ZooKeeper for queuing.
     *
     * @param queueFactory new queue factory
     * @return this (for chaining)
     */
    public WorkflowManagerBuilder withQueueFactory(QueueFactory queueFactory)
    {
        this.queueFactory = Preconditions.checkNotNull(queueFactory, "queueFactory cannot be null");
        return this;
    }

    private WorkflowManagerBuilder()
    {
        try
        {
            instanceName = InetAddress.getLocalHost().getHostName();
        }
        catch ( UnknownHostException e )
        {
            instanceName = "unknown";
        }
    }
}
