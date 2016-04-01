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
import com.nirmata.workflow.admin.WorkflowAdmin;
import com.nirmata.workflow.admin.WorkflowManagerState;
import org.apache.curator.utils.ThreadUtils;
import java.io.Closeable;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class WorkflowManagerStateSampler implements Closeable
{
    private final WorkflowAdmin workflowAdmin;
    private final int windowSize;
    private final Duration samplePeriod;
    private final LinkedList<WorkflowManagerState> samples; // guarded by sync
    private final ScheduledExecutorService service;
    private final AtomicReference<State> state = new AtomicReference<>(State.LATENT);

    private enum State
    {
        LATENT,
        STARTED,
        CLOSED
    }

    public WorkflowManagerStateSampler(WorkflowAdmin workflowAdmin, int windowSize, Duration samplePeriod)
    {
        Preconditions.checkArgument(windowSize > 0, "windowSize must be greater than 0");
        this.workflowAdmin = workflowAdmin;
        this.windowSize = windowSize;
        this.samplePeriod = samplePeriod;
        samples = Lists.newLinkedList();
        service = ThreadUtils.newSingleThreadScheduledExecutor(WorkflowManagerStateSampler.class.getName());
    }

    public void start()
    {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "Already started");
        service.scheduleAtFixedRate(this::runLoop, samplePeriod.toMillis(), samplePeriod.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void close()
    {
        if ( state.compareAndSet(State.STARTED, State.CLOSED) )
        {
            service.shutdownNow();
        }
    }

    public List<WorkflowManagerState> getSamples()
    {
        synchronized(samples)
        {
            return Lists.newArrayList(samples);
        }
    }

    private void runLoop()
    {
        WorkflowManagerState workflowManagerState = workflowAdmin.getWorkflowManagerState();
        synchronized(samples)
        {
            while ( samples.size() >= windowSize )
            {
                samples.removeFirst();
            }
            samples.add(workflowManagerState);
        }
    }
}
