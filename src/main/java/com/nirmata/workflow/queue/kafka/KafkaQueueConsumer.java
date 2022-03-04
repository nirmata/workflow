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

package com.nirmata.workflow.queue.kafka;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.mongodb.MongoInterruptedException;
import com.nirmata.workflow.admin.WorkflowManagerState;
import com.nirmata.workflow.details.KafkaHelper;
import com.nirmata.workflow.models.ExecutableTask;
import com.nirmata.workflow.models.TaskMode;
import com.nirmata.workflow.models.TaskType;
import com.nirmata.workflow.queue.QueueConsumer;
import com.nirmata.workflow.queue.TaskRunner;
import com.nirmata.workflow.serialization.Serializer;
import org.apache.curator.utils.ThreadUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.InterruptException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@VisibleForTesting
public class KafkaQueueConsumer implements Closeable, QueueConsumer {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final KafkaHelper client;
    private final Consumer<String, byte[]> consumer;

    private final TaskRunner taskRunner;
    private final Serializer serializer;
    private final TaskType taskType;
    private final boolean idempotent;

    // PNS Enhancement: Curator ThreadUtils dependency to be changed later
    private final ExecutorService executorService = ThreadUtils.newSingleThreadExecutor("KafkaQueueConsumer");
    private final ExecutorService executorWorkerService;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final NodeFunc nodeFunc;
    private final KeyFunc keyFunc;
    private final AtomicReference<WorkflowManagerState.State> state = new AtomicReference<>(
            WorkflowManagerState.State.LATENT);

    private static final String SEPARATOR = "|";

    @FunctionalInterface
    private interface KeyFunc {
        String apply(String key, long value);
    }

    private static final Map<TaskMode, KeyFunc> keyFuncs;
    static {
        ImmutableMap.Builder<TaskMode, KeyFunc> builder = ImmutableMap.builder();
        builder.put(TaskMode.STANDARD, (key, value) -> key);
        builder.put(TaskMode.PRIORITY, (key, value) -> key + priorityToString(value));
        builder.put(TaskMode.DELAY, (key, value) -> key + epochToString(value));
        keyFuncs = builder.build();
    }

    private static class NodeAndDelay {
        final Optional<String> node;
        final Optional<Long> delay;

        public NodeAndDelay() {
            node = Optional.empty();
            delay = Optional.empty();
        }

        public NodeAndDelay(String node) {
            this.node = Optional.of(node);
            delay = Optional.empty();
        }

        public NodeAndDelay(Optional<String> node) {
            this.node = node;
            delay = Optional.empty();
        }

        public NodeAndDelay(long delay) {
            node = Optional.empty();
            this.delay = Optional.of(delay);
        }
    }

    @FunctionalInterface
    private interface NodeFunc {
        NodeAndDelay getNode(List<String> nodes);
    }

    private static final Map<TaskMode, NodeFunc> nodeFuncs;
    static {
        Random random = new Random();
        ImmutableMap.Builder<TaskMode, NodeFunc> builder = ImmutableMap.builder();
        builder.put(TaskMode.STANDARD, nodes -> new NodeAndDelay(nodes.get(random.nextInt(nodes.size()))));
        builder.put(TaskMode.PRIORITY, nodes -> new NodeAndDelay(nodes.stream().sorted().findFirst()));
        builder.put(TaskMode.DELAY, nodes -> {
            final long sortTime = System.currentTimeMillis();
            Optional<String> first = nodes.stream().sorted(delayComparator(sortTime)).findFirst();
            if (first.isPresent()) {
                long delay = getDelay(first.get(), sortTime);
                return (delay > 0) ? new NodeAndDelay(delay) : new NodeAndDelay(first.get());
            }
            return new NodeAndDelay();
        });
        nodeFuncs = builder.build();
    }

    private static Comparator<String> delayComparator(long sortTime) {
        return (s1, s2) -> {
            long diff = getDelay(s1, sortTime) - getDelay(s2, sortTime);
            return (diff < 0) ? -1 : ((diff > 0) ? 1 : 0);
        };
    }

    private static long getDelay(String itemNode, long sortTime) {
        long epoch = getEpoch(itemNode);
        return epoch - sortTime;
    }

    KafkaQueueConsumer(KafkaHelper client, TaskRunner taskRunner, Serializer serializer, TaskType taskType,
            int executorWorkers) {
        this.client = client;
        this.taskRunner = taskRunner;
        this.serializer = serializer;
        this.taskType = taskType;
        this.idempotent = taskType.isIdempotent();
        this.executorWorkerService = ThreadUtils.newFixedThreadPool(executorWorkers, "KafkaQueueConsumerWorker");

        this.consumer = new KafkaConsumer<String, byte[]>(
                client.getConsumerProps(client.getTaskWorkerConsumerGroup(taskType)));
        client.createTaskTopicIfNeeded(taskType);
        this.consumer.subscribe(Collections.singletonList(client.getTaskExecTopic(taskType)));

        nodeFunc = nodeFuncs.getOrDefault(taskType.getMode(), nodeFuncs.get(TaskMode.STANDARD));
        keyFunc = keyFuncs.getOrDefault(taskType.getMode(), keyFuncs.get(TaskMode.STANDARD));
    }

    @VisibleForTesting
    public static volatile Semaphore debugQueuedTasks = null;

    void put(byte[] data, long value) throws Exception {
        // TODO Internal, Later: This queue is used only on the consumer side
        // (executors)
        // Later use this queue in kafka scheduler too, for consistency of design.
        // Should not be called right now. See Zkp queue equivalent
        throw new UnsupportedOperationException("Internal error. Put side uses Kafka directly");
    }

    @Override
    public WorkflowManagerState.State getState() {
        return state.get();
    }

    @Override
    public void debugValidateClosed() {
        Preconditions.checkState(executorService.isTerminated());
    }

    public void start() {
        if (started.compareAndSet(false, true)) {
            executorService.submit(this::runLoop);
        }
    }

    @Override
    public void close() {
        if (started.compareAndSet(true, false)) {
            executorService.shutdown();
            executorWorkerService.shutdown();
        }
    }

    /**
     * The main consumer loop that polls kafka topic partition for a type, and calls
     * the executor
     */
    private void runLoop() {
        log.info("Starting runLoop");

        try {
            while (started.get() && !Thread.currentThread().isInterrupted()) {
                state.set(WorkflowManagerState.State.SLEEPING);
                try {
                    state.set(WorkflowManagerState.State.SLEEPING);
                    ConsumerRecords<String, byte[]> records = this.consumer.poll(1000);
                    if (records.count() > 0) {
                        state.set(WorkflowManagerState.State.PROCESSING);
                    } else {
                        continue;
                    }

                    for (ConsumerRecord<String, byte[]> record : records) {
                        ExecutableTask task;
                        try {
                            task = serializer.deserialize(record.value(), ExecutableTask.class);
                        } catch (Exception ex) {
                            log.error("Could not deserialize task message, {}, skipping", record.toString(), ex);
                            continue;
                        }
                        submitToWorkerGently(task);
                        // processNode(task);
                    }
                    // TODO Later: Handle priority and delays to extent possible. See equivalent
                    // Zkp implementation. More important is handling fairness. Handle this on the
                    // workflow worker side where DAG is executed and tasks in DAG are submitted for
                    // execution.
                } catch (MongoInterruptedException | InterruptException e) {
                    // log.info("Interrupted runloop", e.getMessage());
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    log.error("Could not process queue", e);
                }
            }
        } finally {
            try {
                this.consumer.close();
            } catch (Exception _e) {
            }
            log.info("Exiting runLoop");
            state.set(WorkflowManagerState.State.CLOSED);
        }
    }

    private void submitToWorkerGently(ExecutableTask task) {
        boolean couldSubmit = false;
        // Submit to executor service with backpressure
        for (int i = 0; !couldSubmit; i++) {
            try {
                executorWorkerService.submit(() -> taskRunner.executeTask(task));
                couldSubmit = true;
            } catch (RejectedExecutionException ex) {
                try {
                    Thread.sleep(1000);
                    if (i % 10 == 0) {
                        log.warn("Slow executors, and more work pumped in");
                    }
                } catch (InterruptedException _ex) {
                }
            } catch (Exception ex) {
                log.error("Error submitting task to executor", ex);
            }
        }
    }

    private void processNode(ExecutableTask executableTask) throws Exception {
        // TODO Later: Handle idempotency. With the kafka design, duplicate task
        // submission should not occur. See equivalent implemention in Zkp code
        try {
            taskRunner.executeTask(executableTask);
        } catch (Throwable e) {
            log.error("Task worker exception in run:task {}:{}. Details: {}", executableTask.getRunId(),
                    executableTask.getTaskId(), e);
        }
    }

    private static String priorityToString(long priority) {
        // the padded hex val of the number prefixed with a 0 for negative numbers
        // and a 1 for positive (so that it sorts correctly)
        long l = priority & 0xFFFFFFFFL;
        return String.format("%s%08X", (priority >= 0) ? "1" : "0", l);
    }

    private static String epochToString(long epoch) {
        return SEPARATOR + String.format("%08X", epoch) + SEPARATOR;
    }

    private static long getEpoch(String itemNode) {
        int index2 = itemNode.lastIndexOf(SEPARATOR);
        int index1 = (index2 > 0) ? itemNode.lastIndexOf(SEPARATOR, index2 - 1) : -1;
        if ((index1 > 0) && (index2 > (index1 + 1))) {
            try {
                String epochStr = itemNode.substring(index1 + 1, index2);
                return Long.parseLong(epochStr, 16);
            } catch (NumberFormatException ignore) {
                // ignore
            }
        }
        return 0;
    }
}
