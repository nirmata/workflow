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
import com.google.common.collect.Sets;
import com.mongodb.MongoInterruptedException;
import com.nirmata.workflow.admin.WorkflowManagerState;
import com.nirmata.workflow.details.internalmodels.RunnableTask;
import com.nirmata.workflow.details.internalmodels.StartedTask;
import com.nirmata.workflow.details.internalmodels.WorkflowMessage;
import com.nirmata.workflow.models.ExecutableTask;
import com.nirmata.workflow.models.RunId;
import com.nirmata.workflow.models.TaskExecutionResult;
import com.nirmata.workflow.models.TaskId;
import com.nirmata.workflow.models.TaskType;
import com.nirmata.workflow.storage.RunRecord;
import com.nirmata.workflow.storage.StorageManager;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.InterruptException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This is a central piece that accepts new task/workflow submissions, figures
 * out next task to run considering dependencies in the task dag, sends tasks to
 * executors, and also gets back task results from executors, to decide the next
 * task to execute, or complete/cancel the workflow
 */
class SchedulerKafka implements Runnable {
    @VisibleForTesting
    static volatile AtomicInteger debugBadRunIdCount;

    private final int MAX_SUBMITTED_CACHE_ITEMS = 10000;
    private final long DEFAULT_KAFKA_POLL_MILLIS = 3000;

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final WorkflowManagerKafkaImpl workflowManager;
    private final StorageManager storageMgr;
    private final AutoCleanerHolder autoCleanerHolder;
    private long pollSleepMillis = DEFAULT_KAFKA_POLL_MILLIS;
    private boolean exitRunLoop = false;
    private Map<TaskType, Producer<String, byte[]>> taskQueues = new HashMap<TaskType, Producer<String, byte[]>>();
    private final Consumer<String, byte[]> workflowConsumer;

    // TODO Enhancement: Have these in-mem caches in persistent storage using a
    // good library such as RocksDB, to avoid dependency on any central data store
    // like Mongo to retry on crashes.
    private Map<String, Map<String, TaskExecutionResult>> completedTasksCache = new HashMap<String, Map<String, TaskExecutionResult>>();
    private Map<String, Set<String>> startedTasksCache = new HashMap<String, Set<String>>();
    private Map<String, RunnableTask> runsCache = new HashMap<String, RunnableTask>();

    // Sufficiently large LRU cache to ensure that duplicate tasks (with same runId)
    // are not scheduled even if scheduled multiple times within a time interval
    // and even if MongoDB is not used as a store.
    // The max size is based on duplicate arrival times to be accomodated.
    // Also expires corresponding old tasks in case they are still running.
    private final Set<String> recentlySubmittedTasks = Collections.newSetFromMap(new LinkedHashMap<String, Boolean>() {
        protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
            if (size() > MAX_SUBMITTED_CACHE_ITEMS) {
                RunId runId = new RunId(eldest.getKey());
                if (runsCache.containsKey(runId.getId())) {
                    RunnableTask runnableTask = runsCache.get(runId.getId());
                    try {
                        // TODO Enhancement: consider storing these in a separate retry Kafka queue, in
                        // case Mongo storage is not active
                        completeRunnableTask(workflowManager, runId, runnableTask, -1);
                    } catch (Exception ex) {
                        log.error("Could not find any data to cancel run: {}", runId, ex);
                    }
                    log.warn("Expiring long running workflow {}", runId);
                }

                return true;
            }
            return false;
        }
    });

    private AtomicReference<WorkflowManagerState.State> state = new AtomicReference<>(
            WorkflowManagerState.State.LATENT);

    // TODO Internal, TBD: Zkp implementation takes an additional queue factory.
    // Don't think that level of customization is needed. We simply queue to kafka.
    // Or maybe, evaluate benefits of queue customization later.
    SchedulerKafka(WorkflowManagerKafkaImpl workflowManager,
            AutoCleanerHolder autoCleanerHolder) {
        this.workflowManager = workflowManager;
        this.storageMgr = workflowManager.getStorageManager();
        this.autoCleanerHolder = autoCleanerHolder;
        this.pollSleepMillis = autoCleanerHolder.getRunPeriod().toMillis() < DEFAULT_KAFKA_POLL_MILLIS
                ? autoCleanerHolder.getRunPeriod().toMillis()
                : DEFAULT_KAFKA_POLL_MILLIS;

        workflowManager.getKafkaConf().createWorkflowTopicIfNeeded();
        this.workflowConsumer = new KafkaConsumer<String, byte[]>(
                workflowManager.getKafkaConf()
                        .getConsumerProps(workflowManager.getKafkaConf().getWorkflowConsumerGroup()));
    }

    public void setExitRunLoop(boolean exitRunLoop) {
        this.exitRunLoop = exitRunLoop;
    }

    WorkflowManagerState.State getState() {
        return state.get();
    }

    /**
     * The main scheduler outer run loop. Poll for messages, and depending upon
     * message type, do the needful.
     * 
     * TODO Scale: One workflow run thread in a client is good enough for 10X
     * or so topic type partitions. Currently, the workflow partitions should be
     * pinned to 1. With Kafka there is uncertainity with respect to partition
     * stickiness to a consumer when consumers join a group gradually. Partition
     * rebalancing might occur.
     * To handle this, we should put consider rebalances as a norm and handle
     * via re-requesting for workflow runs from previous workers for an id via
     * another rebalance topic. All schedulers request runIds for which they
     * suddenly got a result, but no run, over this topic. Schedulers also
     * subscribe to this rebalance topic and if they get a request for rebalance
     * of an Id they had owned previously, they empty those from their caches
     * (runs, started, completed) and send over those records over the workfklw
     * topic as a distinct workflow message so other workflow owner can resume.
     * 
     */
    public void run() {
        int approxLoopCnt = 0;
        log.debug("Starting scheduler run loop");
        this.workflowConsumer.subscribe(Collections.singletonList(workflowManager.getKafkaConf().getWorkflowTopic()));
        try {
            while (!exitRunLoop && !Thread.currentThread().isInterrupted()) {
                if (runsCache.size() > MAX_SUBMITTED_CACHE_ITEMS / 2 && approxLoopCnt % 100 == 0) {
                    log.warn("Active run size increased to {}, more work than what can be consumed", runsCache.size());
                }
                approxLoopCnt++;
                try {
                    state.set(WorkflowManagerState.State.SLEEPING);
                    ConsumerRecords<String, byte[]> records = workflowConsumer
                            .poll(pollSleepMillis);
                    if (records.count() > 0) {
                        state.set(WorkflowManagerState.State.PROCESSING);

                        // TODO Enhancement: Incorporate fairness here. We can take ideas from
                        // Kubernetes
                        // https://kubernetes.io/docs/concepts/cluster-administration/flow-control/

                        for (ConsumerRecord<String, byte[]> record : records) {
                            RunId runId = new RunId(record.key());
                            WorkflowMessage msg;
                            try {
                                msg = workflowManager.getSerializer().deserialize(record.value(),
                                        WorkflowMessage.class);
                            } catch (Exception ex) {
                                log.error("Could not deserialize workflow message, skipping", ex);
                                continue;
                            }
                            log.debug("Deserialized message of type {} from partition {} (key: {}) at offset {}",
                                    msg.getMsgType(), record.partition(), record.key(), record.offset());

                            switch (msg.getMsgType()) {
                                case TASK:
                                    handleTaskMessage(runId, msg);
                                    break;
                                case TASKRESULT:
                                    handleTaskResultMessage(runId, msg);
                                    break;
                                case CANCEL:
                                    handleTaskCancelMessage(runId, msg);
                                    break;
                                // TODO Scale: have a TASKREBALANCE message type case to handle partial
                                // runs that other scheduler had handled, but now the partition is assigned to
                                // this scheduler
                                default:
                                    log.error("Workflow worker received invalid message type for runId {}, {}", runId,
                                            msg.getMsgType());
                                    break;
                            }
                        }
                    }
                    if (autoCleanerHolder.shouldRun()) {
                        autoCleanerHolder.run(workflowManager.getAdmin());
                    }
                    // TODO Scale: later check for requests for a TASKREBALANCE message type
                    // (with a small poll timeout) and if the requested runId has been partially
                    // run, transfer it over to the workflowtopic as a REBALANCE message and clear
                    // our 3 caches.

                } catch (MongoInterruptedException | InterruptException e) {
                    log.debug("Interrupted scheduler loop", e.getMessage());
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    log.error("Error while running scheduler", e);
                }
            }
        } finally {
            log.debug("Closing workflow consumer");
            try {
                workflowConsumer.close();
            } catch (Exception _e) {
            }
            state.set(WorkflowManagerState.State.CLOSED);
        }

    }

    private void handleTaskMessage(RunId runId, WorkflowMessage msg) {
        if (runsCache.size() >= MAX_SUBMITTED_CACHE_ITEMS) {
            // TODO Enhancement: consider storing these in a separate retry Kafka queue, in
            // case Mongo storage is not active and we need retries.
            log.warn(
                    "Active run size reached threshold of {}, dropping scheduling RunId {}, resubmit later",
                    MAX_SUBMITTED_CACHE_ITEMS, runId);
            return;
        }
        if (!msg.isRetry()) {
            if (!runsCache.containsKey(runId.getId())) {
                completedTasksCache.put(runId.getId(),
                        new HashMap<String, TaskExecutionResult>());
                startedTasksCache.put(runId.getId(), new HashSet<String>());
                runsCache.put(runId.getId(), msg.getRunnableTask().get());
                // Adding same key does not change LRU order, hence delete and add
                recentlySubmittedTasks.remove(runId.getId());
                recentlySubmittedTasks.add(runId.getId());
            } else {
                log.debug("Ignoring duplicate task submitted for run {}", runId);
                return;
            }
        } else {
            // Someone retrying this workflow. Perhaps some old workflow run died
            populateCacheFromDb(runId);
        }
        updateTasks(runId);
    }

    private void handleTaskResultMessage(RunId runId, WorkflowMessage msg) {
        if (runsCache.containsKey(runId.getId())) {
            completedTasksCache.get(runId.getId()).put(msg.getTaskId().get().getId(),
                    msg.getTaskExecResult().get());
            updateTasks(runId);
        } else {
            // A task result was received for run that I don't have
            // Mostly some partition reassignment, or cancelled run.
            // Wait for someone to resubmit the job
            log.warn(
                    "Got result, but no runId for {}, ignoring. Repartition due to failure or residual in Kafka due to late autocommit?",
                    runId.getId());
            // TODO Scale: broadcast this taskId to a new task rebalance topic to request
            // other schedulers to send across partial runs for this runId if they have them
        }
    }

    private void handleTaskCancelMessage(RunId runId, WorkflowMessage msg) {
        byte[] runnableBytes = storageMgr.getRunnable(runId);
        try {
            completeRunnableTask(workflowManager, runId,
                    runnableBytes == null ? null
                            : workflowManager.getSerializer().deserialize(
                                    runnableBytes, RunnableTask.class),
                    -1);
        } catch (Exception ex) {
            log.error("Could not find any data to cancel run: {}", runId, ex);
        }
    }

    /**
     * Fetch run related details from the DB and populate in-mem caches
     * 
     * @param runId
     */
    private void populateCacheFromDb(RunId runId) {
        try {
            RunRecord runRec = storageMgr.getRunDetails(runId);
            if (runRec == null) {
                log.error("Unexpected state. Did not find runId in DB: {}", runId);
                return;
            }

            completedTasksCache.put(runId.getId(), new HashMap<String, TaskExecutionResult>());
            startedTasksCache.put(runId.getId(), new HashSet<String>());

            RunnableTask runnableTask = workflowManager.getSerializer().deserialize(runRec.getRunnableData(),
                    RunnableTask.class);
            runsCache.put(runId.getId(), runnableTask);

            for (Map.Entry<String, byte[]> entry : runRec.getCompletedTasks().entrySet()) {
                TaskId taskId = new TaskId(entry.getKey());

                try {
                    TaskExecutionResult taskExecutionResult = workflowManager.getSerializer().deserialize(
                            entry.getValue(),
                            TaskExecutionResult.class);
                    completedTasksCache.get(runId.getId()).put(taskId.getId(), taskExecutionResult);
                } catch (Exception e) {
                    throw new RuntimeException("Trying to read started task info for task: " + taskId, e);
                }
            }

        } catch (Exception e) {
            log.error("Error creating Runnable from DB record for runId: {}", runId, e);
        }
    }

    private boolean hasCanceledTasks(RunId runId, RunnableTask runnableTask) {
        return runnableTask.getTasks().keySet().stream().anyMatch(taskId -> {
            TaskExecutionResult taskExecutionResult = completedTasksCache.get(runId.getId()).get(taskId.getId());
            if (taskExecutionResult != null) {
                return taskExecutionResult.getStatus().isCancelingStatus();
            }
            return false;
        });
    }

    private void completeRunnableTask(WorkflowManagerKafkaImpl workflowManager, RunId runId,
            RunnableTask runnableTask, int version) {
        runsCache.remove(runId.getId());
        startedTasksCache.remove(runId.getId());
        completedTasksCache.remove(runId.getId());

        log.info("Completing run: {}", runId);
        if (runnableTask == null) {
            return;
        }

        try {
            RunId parentRunId = runnableTask.getParentRunId().orElse(null);
            RunnableTask completedRunnableTask = new RunnableTask(runnableTask.getTasks(), runnableTask.getTaskDags(),
                    runnableTask.getStartTimeUtc(), LocalDateTime.now(Clock.systemUTC()), parentRunId);
            byte[] json = workflowManager.getSerializer().serialize(completedRunnableTask);
            // For child runids (sub-workflows), we need to maintain information in cache
            // till parent finishes
            if (runnableTask.getParentRunId().isPresent()) {
                runsCache.put(runId.getId(), completedRunnableTask);
            }
            storageMgr.updateRun(runId, json);
        } catch (Exception e) {
            String message = "Could not write completed task data for run: " + runId;
            log.error(message, e);
            throw new RuntimeException(message, e);
        }
    }

    /**
     * This method has the main DAG progress logic based on task dependencies,
     * completed tasks, etc.
     */
    private void updateTasks(RunId runId) {
        log.debug("Updating run: " + runId);

        RunnableTask runnableTask = getRunnableTask(runId);

        if (runnableTask == null) {
            log.warn("No runnable task for runId {}, skipping update", runId);
            return;
        }

        if (runnableTask.getCompletionTimeUtc().isPresent()) {
            log.debug("Run is completed. Ignoring: " + runId);
            return;
        }

        if (hasCanceledTasks(runId, runnableTask)) {
            log.debug("Run has canceled tasks and will be marked completed: {}", runId);
            completeRunnableTask(workflowManager, runId, runnableTask, -1);
            return; // one or more tasks have canceled the entire run
        }

        Set<TaskId> completedTasksForRun = Sets.newHashSet();
        runnableTask.getTaskDags().forEach(entry -> {
            TaskId taskId = entry.getTaskId();
            ExecutableTask task = runnableTask.getTasks().get(taskId);
            if (task == null) {
                log.error(String.format("Could not find task: %s for run: %s", taskId, runId));
                return;
            }

            boolean taskIsComplete = taskIsComplete(runId, task);
            if (taskIsComplete) {
                completedTasksForRun.add(taskId);
            } else if (!taskIsStarted(runId, taskId)) {
                boolean allDependenciesAreComplete = entry
                        .getDependencies()
                        .stream()
                        .allMatch(id -> taskIsComplete(runId, runnableTask.getTasks().get(id)));
                if (allDependenciesAreComplete) {
                    queueTask(runId, task);
                    clearCompletedChildRuns(runId, runnableTask);
                }
            }
        });

        if (completedTasksForRun.equals(runnableTask.getTasks().keySet())) {
            completeRunnableTask(workflowManager, runId, runnableTask, -1);
            // If a child workflow has completed, then, it might have unblocked
            // parents also, so try updateTasks for that parent again
            if (runnableTask.getParentRunId().isPresent()) {
                updateTasks(runnableTask.getParentRunId().get());
            }
        }
    }

    /**
     * A child run might have been maintained in memory in order to track parent
     * state. Clear this if parent has finished.
     */
    private void clearCompletedChildRuns(RunId runId, RunnableTask runnableTask) {
        runnableTask.getTaskDags().forEach(entry -> {
            for (TaskId dagTid : entry.getDependencies()) {
                ExecutableTask task = runnableTask.getTasks().get(dagTid);
                TaskExecutionResult result = completedTasksCache.get(runId.getId()).get(task.getTaskId().getId());
                if (result != null) {
                    if (result.getSubTaskRunId().isPresent()) {
                        runsCache.remove(result.getSubTaskRunId().get().getId());
                    }
                }
            }
        });

    }

    private RunnableTask getRunnableTask(RunId runId) {
        return runsCache.get(runId.getId());
    }

    /**
     * Send task to executors via Kafka.
     */
    private void queueTask(RunId runId, ExecutableTask task) {
        try {
            // TODO Later: Incorporate delayed tasks here somehow??.

            StartedTask startedTask = new StartedTask(workflowManager.getInstanceName(),
                    LocalDateTime.now(Clock.systemUTC()), 0);
            storageMgr.setStartedTask(runId, task.getTaskId(), workflowManager.getSerializer().serialize(startedTask));

            byte[] runnableTaskBytes = workflowManager.getSerializer().serialize(task);

            Producer<String, byte[]> producer = taskQueues.get(task.getTaskType());
            if (producer == null) {
                workflowManager.getKafkaConf().createTaskTopicIfNeeded(task.getTaskType());
                producer = new KafkaProducer<String, byte[]>(
                        workflowManager.getKafkaConf().getProducerProps());
                taskQueues.put(task.getTaskType(), producer);
            }

            producer.send(new ProducerRecord<String, byte[]>(
                    workflowManager.getKafkaConf().getTaskExecTopic(task.getTaskType()), runnableTaskBytes),
                    new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata m, Exception e) {
                            if (e != null) {
                                log.error("Error creating record for Run {} to task type {}", runId, task.getTaskType(),
                                        e);
                            } else {
                                log.debug("RunId {} produced record to topic {}, partition [{}] @ offset {}", runId,
                                        m.topic(), m.partition(), m.offset());
                            }
                        }
                    });
            startedTasksCache.get(runId.getId()).add(task.getTaskId().getId());
            log.debug("Sent task to queue: {}", task);
        } catch (Exception e) {
            String message = "Could not start task " + task;
            log.error(message, e);
            throw new RuntimeException(e);
        }
    }

    private boolean taskIsStarted(RunId runId, TaskId taskId) {
        return startedTasksCache.get(runId.getId()).contains(taskId.getId());
    }

    private boolean taskIsComplete(RunId runId, ExecutableTask task) {
        if ((task == null) || !task.isExecutable()) {
            return true;
        }

        TaskExecutionResult result = completedTasksCache.get(runId.getId()).get(task.getTaskId().getId());
        if (result != null) {
            if (result.getSubTaskRunId().isPresent()) {
                RunnableTask runnableTask = getRunnableTask(result.getSubTaskRunId().get());
                return (runnableTask != null) && runnableTask.getCompletionTimeUtc().isPresent();
            }
            return true;
        }
        return false;
    }
}
