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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.mongodb.MongoInterruptedException;
import com.nirmata.workflow.WorkflowManager;
import com.nirmata.workflow.admin.RunInfo;
import com.nirmata.workflow.admin.TaskDetails;
import com.nirmata.workflow.admin.TaskInfo;
import com.nirmata.workflow.admin.WorkflowAdmin;
import com.nirmata.workflow.admin.WorkflowManagerState;
import com.nirmata.workflow.details.internalmodels.RunnableTask;
import com.nirmata.workflow.details.internalmodels.StartedTask;
import com.nirmata.workflow.details.internalmodels.WorkflowMessage;
import com.nirmata.workflow.events.WorkflowListenerManager;
import com.nirmata.workflow.executor.TaskExecution;
import com.nirmata.workflow.executor.TaskExecutor;
import com.nirmata.workflow.models.ExecutableTask;
import com.nirmata.workflow.models.RunId;
import com.nirmata.workflow.models.Task;
import com.nirmata.workflow.models.TaskExecutionResult;
import com.nirmata.workflow.models.TaskId;
import com.nirmata.workflow.models.TaskType;
import com.nirmata.workflow.queue.QueueConsumer;
import com.nirmata.workflow.queue.QueueFactory;
import com.nirmata.workflow.serialization.Serializer;
import com.nirmata.workflow.storage.RunRecord;
import com.nirmata.workflow.storage.StorageManager;

import org.apache.curator.utils.CloseableUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Implementation of the covering WorkflowManager interface using Kafka
 */
public class WorkflowManagerKafkaImpl implements WorkflowManager, WorkflowAdmin {

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final String instanceName;
    private final List<QueueConsumer> consumers;
    private final KafkaHelper kafkaHelper;
    private final StorageManager storageMgr;
    private final boolean workflowWorkerEnabled;
    Producer<String, byte[]> wflowProducer;

    private final SchedulerSelectorKafka schedulerSelector;
    private final AtomicReference<State> state = new AtomicReference<>(State.LATENT);
    private final Serializer serializer;
    private final Executor taskRunnerService;

    private static final TaskType nullTaskType = new TaskType("", "", false);

    private enum State {
        LATENT,
        STARTED,
        CLOSED
    }

    public WorkflowManagerKafkaImpl(KafkaHelper kafkaConf, StorageManager storageMgr,
            boolean workflowWorkerEnabled, QueueFactory queueFactory,
            String instanceName, List<TaskExecutorSpec> specs, AutoCleanerHolder autoCleanerHolder,
            Serializer serializer, Executor taskRunnerService) {
        this.taskRunnerService = Preconditions.checkNotNull(taskRunnerService, "taskRunnerService cannot be null");
        this.serializer = Preconditions.checkNotNull(serializer, "serializer cannot be null");
        autoCleanerHolder = Preconditions.checkNotNull(autoCleanerHolder, "autoCleanerHolder cannot be null");

        this.kafkaHelper = Preconditions.checkNotNull(kafkaConf, "kafka props cannot be null");
        this.storageMgr = Preconditions.checkNotNull(storageMgr, "storage manager cannot be null");
        this.workflowWorkerEnabled = workflowWorkerEnabled;
        wflowProducer = new KafkaProducer<String, byte[]>(this.kafkaHelper.getProducerProps());

        queueFactory = Preconditions.checkNotNull(queueFactory, "queueFactory cannot be null");
        this.instanceName = Preconditions.checkNotNull(instanceName, "instanceName cannot be null");
        specs = Preconditions.checkNotNull(specs, "specs cannot be null");

        consumers = makeTaskConsumers(queueFactory, specs);
        schedulerSelector = new SchedulerSelectorKafka(this, queueFactory, autoCleanerHolder);
    }

    public KafkaHelper getKafkaConf() {
        return this.kafkaHelper;
    }

    public StorageManager getStorageManager() {
        return this.storageMgr;
    }

    @VisibleForTesting
    volatile boolean debugDontStartConsumers = false;

    @Override
    public void start() {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "Already started");

        if (!debugDontStartConsumers) {
            startQueueConsumers();
        }
        if (workflowWorkerEnabled) {
            kafkaHelper.createWorkflowTopicIfNeeded();
            schedulerSelector.start();
        }
    }

    @VisibleForTesting
    void startQueueConsumers() {
        consumers.forEach(QueueConsumer::start);
    }

    @Override
    public WorkflowListenerManager newWorkflowListenerManager() {
        // TODO: Later. Unsupported for now. Provide support for Kafka workflow
        // listener. Currently this interface is not used by any client service.
        // The Kafka workflow can write status of completed runs to a Kafka topic
        // called completed runs and this listener implementation would involve
        // listening to that. Need new listener manager equivalent to
        // WorkflowListenerManagerImpl using Zkp
        throw new UnsupportedOperationException("Listeners on Kafka workflows not yet supported");
    }

    @Override
    public Map<TaskId, TaskDetails> getTaskDetails(RunId runId) {
        try {
            byte[] runnableTaskBytes = this.storageMgr.getRunnable(runId);
            if (runnableTaskBytes != null) {
                RunnableTask runnableTask = serializer.deserialize(runnableTaskBytes, RunnableTask.class);
                return runnableTask.getTasks()
                        .entrySet()
                        .stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, entry -> {
                            ExecutableTask executableTask = entry.getValue();
                            TaskType taskType = executableTask.getTaskType().equals(nullTaskType) ? null
                                    : executableTask.getTaskType();
                            return new TaskDetails(entry.getKey(), taskType, executableTask.getMetaData());
                        }));
            }
        } catch (Exception e) {
            log.error("Error getting task details for runId {}", runId, e);
        }
        return new HashMap<TaskId, TaskDetails>();
    }

    @Override
    public RunId submitTask(Task task) {
        return submitSubTask(new RunId(), null, task);
    }

    @Override
    public RunId submitTask(RunId runId, Task task) {
        return submitSubTask(runId, null, task);
    }

    @Override
    public RunId submitSubTask(RunId parentRunId, Task task) {
        return submitSubTask(new RunId(), parentRunId, task);
    }

    public volatile long debugLastSubmittedTimeMs = 0;

    @Override
    public RunId submitSubTask(RunId runId, RunId parentRunId, Task task) {
        Preconditions.checkState(state.get() == State.STARTED, "Not started");

        RunnableTaskDagBuilder builder = new RunnableTaskDagBuilder(task);
        Map<TaskId, ExecutableTask> tasks = builder
                .getTasks()
                .values()
                .stream()
                .collect(Collectors.toMap(Task::getTaskId, t -> new ExecutableTask(runId, t.getTaskId(),
                        t.isExecutable() ? t.getTaskType() : nullTaskType, t.getMetaData(), t.isExecutable())));
        RunnableTask runnableTask = new RunnableTask(tasks, builder.getEntries(), LocalDateTime.now(ZoneOffset.UTC),
                null,
                parentRunId);

        try {
            WorkflowMessage wm = new WorkflowMessage(runnableTask);
            byte[] workflowMsg = serializer.serialize(wm);
            debugLastSubmittedTimeMs = System.currentTimeMillis();
            storageMgr.createRun(runId, serializer.serialize(runnableTask));
            sendWorkflowToKafka(runId, workflowMsg);
        } catch (Exception e) {
            log.error("Could not submit workflow for runId {} to Kafka", runId, e);
            throw e;
        }

        return runId;
    }

    // Put the workflow dag in Kafka
    private void sendWorkflowToKafka(RunId runId, byte[] workflowMsgBytes) {
        wflowProducer.send(
                new ProducerRecord<String, byte[]>(
                        kafkaHelper.getWorkflowTopic(),
                        runId.getId(),
                        workflowMsgBytes),
                new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata m, Exception e) {
                        if (e != null) {
                            log.error("Error creating record for Run {} to topic {}", runId,
                                    kafkaHelper.getWorkflowTopic(), e);
                        } else {
                            log.debug("RunId {} produced record to topic {}, partition [{}] @ offset {}", runId,
                                    m.topic(), m.partition(), m.offset());
                        }
                    }
                });

    }

    public void updateTaskProgress(RunId runId, TaskId taskId, int progress) {
        Preconditions.checkArgument((progress >= 0) && (progress <= 100), "progress must be between 0 and 100");

        try {
            byte[] bytes = storageMgr.getStartedTask(runId, taskId);
            if (bytes != null) {
                StartedTask startedTask = serializer.deserialize(bytes, StartedTask.class);
                StartedTask updatedStartedTask = new StartedTask(startedTask.getInstanceName(),
                        startedTask.getStartDateUtc(), progress);
                byte[] data = getSerializer().serialize(updatedStartedTask);
                storageMgr.setStartedTask(runId, taskId, data);
            }
        } catch (Exception e) {
            log.error("Error updating task progress for runId:taskId {}:{}", runId, taskId, e);
            throw new RuntimeException("Trying to update task info for " + runId + ":" + taskId + ":" + e);
        }
    }

    @Override
    public boolean cancelRun(RunId runId) {
        log.info("Attempting to cancel run " + runId);

        byte[] bytes = serializer.serialize(new WorkflowMessage());
        try {
            sendWorkflowToKafka(runId, bytes);
        } catch (Exception e) {
            log.error("Could not cancel workflow {}", runId, e);
            return false;
        }

        return true;
    }

    @Override
    public Optional<TaskExecutionResult> getTaskExecutionResult(RunId runId, TaskId taskId) {
        try {
            byte[] bytes = storageMgr.getTaskExecutionResult(runId, taskId);
            return Optional.ofNullable(bytes == null ? null : serializer.deserialize(bytes, TaskExecutionResult.class));
        } catch (Exception e) {
            log.error("No execution result found for runId {} taskId {}", runId, taskId, e);
        }
        return Optional.of(null);
    }

    public String getInstanceName() {
        return instanceName;
    }

    @VisibleForTesting
    public void debugValidateClosed() {
        consumers.forEach(QueueConsumer::debugValidateClosed);
        schedulerSelector.debugValidateClosed();
    }

    @Override
    public void close() {
        if (state.compareAndSet(State.STARTED, State.CLOSED)) {
            CloseableUtils.closeQuietly(schedulerSelector);
            consumers.forEach(CloseableUtils::closeQuietly);
        }
    }

    @Override
    public WorkflowAdmin getAdmin() {
        return this;
    }

    @Override
    public WorkflowManagerState getWorkflowManagerState() {
        return new WorkflowManagerState(
                false, // Zkp is NA. And it is not continuously connected to Kafka.
                schedulerSelector.getState(),
                consumers.stream().map(QueueConsumer::getState).collect(Collectors.toList()));
    }

    @Override
    public boolean clean(RunId runId) {
        return storageMgr.clean(runId);
    }

    @Override
    public RunInfo getRunInfo(RunId runId) {
        try {
            byte[] bytes = storageMgr.getRunnable(runId);
            if (bytes != null) {
                RunnableTask runnableTask = serializer.deserialize(bytes, RunnableTask.class);
                return new RunInfo(runId, runnableTask.getStartTimeUtc(),
                        runnableTask.getCompletionTimeUtc().orElse(null));
            }
        } catch (Exception e) {
            log.error("Error getting RunInfo for runId: {}", runId, e);
        }
        return null;
    }

    @Override
    public List<RunId> getRunIds() {
        try {
            return storageMgr.getRunIds().stream()
                    .map(RunId::new)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            log.error("Error getting all RunIds", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<RunInfo> getRunInfo() {
        try {
            return storageMgr.getRuns().entrySet().stream()
                    .map(entry -> {
                        RunId runId = new RunId(entry.getKey());
                        try {
                            RunnableTask runnableTask = serializer.deserialize(entry.getValue(), RunnableTask.class);
                            return new RunInfo(runId, runnableTask.getStartTimeUtc(),
                                    runnableTask.getCompletionTimeUtc().orElse(null));
                        } catch (Exception e) {
                            throw new RuntimeException("Trying to read run info for runId " + runId, e);
                        }
                    })
                    .filter(info -> (info != null))
                    .collect(Collectors.toList());
        } catch (Throwable e) {
            log.error("Error getting RunInfo for all runs", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<TaskInfo> getTaskInfo(RunId runId) {
        List<TaskInfo> taskInfos = Lists.newArrayList();
        RunRecord runRec = storageMgr.getRunDetails(runId);
        if (runRec == null) {
            return taskInfos;
        }

        try {
            RunnableTask runnableTask = serializer.deserialize(runRec.getRunnableData(), RunnableTask.class);

            Set<TaskId> notStartedTasks = runnableTask.getTasks().values().stream().filter(ExecutableTask::isExecutable)
                    .map(ExecutableTask::getTaskId).collect(Collectors.toSet());
            Map<TaskId, StartedTask> startedTasks = Maps.newHashMap();

            for (Map.Entry<String, byte[]> entry : runRec.getStartedTasks().entrySet()) {
                TaskId taskId = new TaskId(entry.getKey());
                try {
                    StartedTask startedTask = serializer.deserialize(entry.getValue(), StartedTask.class);
                    startedTasks.put(taskId, startedTask);
                    notStartedTasks.remove(taskId);
                } catch (Exception e) {
                    throw new RuntimeException("Trying to read started task info for task: " + taskId, e);
                }
            }

            for (Map.Entry<String, byte[]> entry : runRec.getCompletedTasks().entrySet()) {
                TaskId taskId = new TaskId(entry.getKey());

                StartedTask startedTask = startedTasks.remove(taskId);
                if (startedTask != null) // otherwise it must have been deleted
                {
                    try {
                        TaskExecutionResult taskExecutionResult = serializer.deserialize(entry.getValue(),
                                TaskExecutionResult.class);
                        taskInfos.add(new TaskInfo(taskId, startedTask.getInstanceName(), startedTask.getStartDateUtc(),
                                startedTask.getProgress(), taskExecutionResult));
                        notStartedTasks.remove(taskId);
                    } catch (Exception e) {
                        throw new RuntimeException("Trying to read started task info for task: " + taskId, e);
                    }
                }
            }

            // remaining started tasks have not completed
            startedTasks.forEach((key, startedTask) -> taskInfos.add(new TaskInfo(key, startedTask.getInstanceName(),
                    startedTask.getStartDateUtc(), startedTask.getProgress())));

            // finally, taskIds not added have not started
            notStartedTasks.forEach(taskId -> taskInfos.add(new TaskInfo(taskId)));
        } catch (Throwable e) {
            log.error("Error getting TaskInfo for runId {}}", runId, e);
            throw new RuntimeException(e);
        }
        return taskInfos;
    }

    public Serializer getSerializer() {
        return serializer;
    }

    @VisibleForTesting
    SchedulerSelectorKafka getSchedulerSelector() {
        return schedulerSelector;
    }

    private void executeTask(TaskExecutor taskExecutor, ExecutableTask executableTask) {
        if (state.get() != State.STARTED) {
            return;
        }

        log.debug("Executing task: {}", executableTask);
        TaskExecution taskExecution = taskExecutor.newTaskExecution(this, executableTask);

        TaskExecutionResult result;
        try {
            FutureTask<TaskExecutionResult> futureTask = new FutureTask<>(taskExecution::execute);
            taskRunnerService.execute(futureTask);
            result = futureTask.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        } catch (ExecutionException e) {
            log.error("Could not execute task: " + executableTask, e);
            throw new RuntimeException(e);
        }
        if (result == null) {
            throw new RuntimeException(String.format("null returned from task executor for run: %s, task %s",
                    executableTask.getRunId(), executableTask.getTaskId()));
        }
        byte[] bytes = serializer.serialize(new WorkflowMessage(executableTask.getTaskId(), result));
        try {
            // Send task result to scheduler to further advance the workflow
            sendWorkflowToKafka(executableTask.getRunId(), bytes);
            storageMgr.saveTaskResult(executableTask.getRunId(), executableTask.getTaskId(),
                    serializer.serialize(result));

        } catch (Exception e) {
            log.error("Could not set completed data for executable task: {}", executableTask, e);
            throw e;
        }
    }

    /**
     * Create multiple queue consumers to read from a particular topictype partition
     * and run executors on the message received.
     */
    private List<QueueConsumer> makeTaskConsumers(QueueFactory queueFactory, List<TaskExecutorSpec> specs) {
        ImmutableList.Builder<QueueConsumer> builder = ImmutableList.builder();
        // When we have sufficient partitions so that each thread can have its
        // own consumer, we change the (0, 1) to (0, spec.getQty()), and pass 1
        // (or nothing) in the createConsumer instead. One partition for all task
        // executors might become a bottleneck when the task execution itself is
        // relatively as fast as the part that fetches from the queue and derserializes
        // message
        specs.forEach(spec -> IntStream.range(0, 1).forEach(i -> {

            QueueConsumer consumer = queueFactory.createQueueConsumer(this,
                    t -> executeTask(spec.getTaskExecutor(), t),
                    spec.getTaskType(), spec.getQty());
            builder.add(consumer);
        }));

        return builder.build();
    }
}
