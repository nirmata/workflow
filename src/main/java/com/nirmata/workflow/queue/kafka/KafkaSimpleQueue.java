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

import com.google.common.base.Preconditions;
import com.nirmata.workflow.details.KafkaHelper;
import com.nirmata.workflow.models.ExecutableTask;
import com.nirmata.workflow.models.TaskType;
import com.nirmata.workflow.queue.Queue;
import com.nirmata.workflow.queue.TaskRunner;
import com.nirmata.workflow.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Kafka simple queue, only wraps the queue consumer. The producer side
 * is directly handled by insertion in the appropriate Kafka topic. Kafka topic
 * queues can be folded into a generic queue later. Additional comments in
 * KafkaQueueConsumer
 */
public class KafkaSimpleQueue implements Queue {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final KafkaQueueConsumer queue;
    private final Serializer serializer;

    KafkaSimpleQueue(TaskRunner taskRunner, Serializer serializer, KafkaHelper kafkaHlpr, TaskType taskType,
            int runnerQty) {
        this.serializer = Preconditions.checkNotNull(serializer, "serializer cannot be null");
        queue = new KafkaQueueConsumer(kafkaHlpr, taskRunner, serializer, taskType, runnerQty);
    }

    @Override
    public void start() {
        // NOP
    }

    @Override
    public void close() {
        // NOP
    }

    @Override
    public void put(ExecutableTask executableTask) {
        // TODO: Later. Currently the send side of queue is directly done in Kafka. This
        // function should not be executed till then. See Zkp implementation
        throw new UnsupportedOperationException("Internal error. Put side uses Kafka directly");
    }

    KafkaQueueConsumer getQueue() {
        return queue;
    }

    Serializer getSerializer() {
        return serializer;
    }

}
