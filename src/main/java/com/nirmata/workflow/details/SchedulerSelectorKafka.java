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
import com.nirmata.workflow.admin.WorkflowManagerState;
import com.nirmata.workflow.queue.QueueFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * TODO Internal Later, if the queue interface and related threading is not
 * implemented this class can be removed and the scheduler used directly.
 * Especially since leader election is irrelevant in Kafka. Right now, this just
 * houses the underlying Scheduler thread
 */
public class SchedulerSelectorKafka implements Closeable {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final WorkflowManagerKafkaImpl workflowManager;
    private final AutoCleanerHolder autoCleanerHolder;
    private SchedulerKafka scheduler; // = new AtomicReference<>();
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    public SchedulerSelectorKafka(WorkflowManagerKafkaImpl workflowManager, QueueFactory queueFactory,
            AutoCleanerHolder autoCleanerHolder) {
        this.workflowManager = workflowManager;
        this.autoCleanerHolder = autoCleanerHolder;
    }

    public WorkflowManagerState.State getState() {
        return (scheduler != null) ? scheduler.getState() : WorkflowManagerState.State.LATENT;
    }

    public void start() {
        // Zkp implementation needs to do leader selection. Not needed in Kafka.
        // Only one or few workflow runners need to be present based on partitions of
        // the workflow topic. All extra consumers for a workflow queue exceeding
        // partitions will be idle till someone dies.
        // Note: In Kafka 0.10.*, Kafka's determination of dead consumers is a bit
        // finicky, might reassign partitions suddenly (say some poll or timeout
        // criteria) so, best to have one workflow partition. In later versions of
        // Kafka, we could use the cooperative sticky assignor.
        this.scheduler = new SchedulerKafka(workflowManager, autoCleanerHolder);
        log.info(workflowManager.getInstanceName() + " ready to act as scheduler");
        executorService.execute(scheduler);
    }

    @Override
    public void close() {

        if (scheduler == null) {
            return;
        }

        try {
            log.debug("Shutting down Scheduler service");
            scheduler.setExitRunLoop(true);
            executorService.shutdown();
            scheduler = null;

            if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                log.warn("Could not shutdown scheduler service cleanly");
            }
        } catch (Exception ex) {
            log.debug("Exception waiting for scheduler shutdown", ex);
        }
    }

    @VisibleForTesting
    void debugValidateClosed() {
    }

}
