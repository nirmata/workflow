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
import com.nirmata.workflow.admin.WorkflowManagerState;
import com.nirmata.workflow.queue.QueueFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.utils.CloseableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public class SchedulerSelector implements Closeable
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final WorkflowManagerImpl workflowManager;
    private final QueueFactory queueFactory;
    private final AutoCleanerHolder autoCleanerHolder;
    private final LeaderSelector leaderSelector;
    private final AtomicReference<Scheduler> scheduler = new AtomicReference<>();

    volatile AtomicReference<CountDownLatch> debugLatch = new AtomicReference<>();

    public SchedulerSelector(WorkflowManagerImpl workflowManager, QueueFactory queueFactory, AutoCleanerHolder autoCleanerHolder)
    {
        this.workflowManager = workflowManager;
        this.queueFactory = queueFactory;
        this.autoCleanerHolder = autoCleanerHolder;

        LeaderSelectorListener listener = new LeaderSelectorListenerAdapter()
        {
            @Override
            public void takeLeadership(CuratorFramework client) throws Exception
            {
                SchedulerSelector.this.takeLeadership();
            }
        };
        leaderSelector = new LeaderSelector(workflowManager.getCurator(), ZooKeeperConstants.getSchedulerLeaderPath(), listener);
        leaderSelector.autoRequeue();
    }

    public WorkflowManagerState.State getState()
    {
        Scheduler localScheduler = scheduler.get();
        return (localScheduler != null) ? localScheduler.getState() : WorkflowManagerState.State.LATENT;
    }

    public void start()
    {
        leaderSelector.start();
    }

    @Override
    public void close()
    {
        CloseableUtils.closeQuietly(leaderSelector);
    }

    @VisibleForTesting
    LeaderSelector getLeaderSelector()
    {
        return leaderSelector;
    }

    @VisibleForTesting
    void debugValidateClosed()
    {
        Preconditions.checkState(!leaderSelector.hasLeadership());
    }

    private void takeLeadership()
    {
        log.info(workflowManager.getInstanceName() + " is now the scheduler");
        try
        {
            scheduler.set(new Scheduler(workflowManager, queueFactory, autoCleanerHolder));
            scheduler.get().run();
        }
        finally
        {
            log.info(workflowManager.getInstanceName() + " is no longer the scheduler");
            scheduler.set(null);

            CountDownLatch latch = debugLatch.getAndSet(null);
            if ( latch != null )
            {
                latch.countDown();
            }
        }
    }
}
