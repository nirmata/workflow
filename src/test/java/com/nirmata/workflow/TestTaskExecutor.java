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

import com.google.common.collect.Maps;
import com.nirmata.workflow.executor.TaskExecution;
import com.nirmata.workflow.executor.TaskExecutionStatus;
import com.nirmata.workflow.executor.TaskExecutor;
import com.nirmata.workflow.models.ExecutableTask;
import com.nirmata.workflow.models.TaskExecutionResult;
import java.util.concurrent.CountDownLatch;

public class TestTaskExecutor implements TaskExecutor {
    private final ConcurrentTaskChecker checker = new ConcurrentTaskChecker();
    private final int latchQty;
    private volatile CountDownLatch latch;
    private final boolean checkerActive;
    private final int sleepMillis;

    public TestTaskExecutor() {
        this(1, true, 1000);
    }

    public TestTaskExecutor(int numExecutors) {
        this(numExecutors, true, 1000);
    }

    public TestTaskExecutor(int numExecutors, boolean checkerActive) {
        this(numExecutors, checkerActive, 1000);
    }

    public TestTaskExecutor(int latchQty, boolean checkerActive, int sleepMillis) {
        this.latchQty = latchQty;
        latch = new CountDownLatch(latchQty);
        this.checkerActive = checkerActive;
        this.sleepMillis = sleepMillis;
    }

    public CountDownLatch getLatch() {
        return latch;
    }

    public ConcurrentTaskChecker getChecker() {
        return checker;
    }

    public void reset() {
        checker.reset();
        latch = new CountDownLatch(latchQty);
    }

    @Override
    public TaskExecution newTaskExecution(WorkflowManager workflowManager, ExecutableTask task) {
        return () -> {
            try {
                if (checkerActive) {
                    checker.add(task.getTaskId());
                }
                doRun(task);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } finally {
                checker.decrement();
                latch.countDown();
            }
            return new TaskExecutionResult(TaskExecutionStatus.SUCCESS, "hey", Maps.<String, String>newHashMap());
        };
    }

    @SuppressWarnings("UnusedParameters")
    protected void doRun(ExecutableTask task) throws InterruptedException {
        if (this.sleepMillis > 0) {
            Thread.sleep(this.sleepMillis);
        }
    }
}
