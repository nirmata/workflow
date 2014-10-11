package com.nirmata.workflow.details;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.nirmata.workflow.queue.QueueFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.utils.CloseableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public class SchedulerSelector implements Closeable
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final WorkflowManagerImpl workflowManager;
    private final QueueFactory queueFactory;
    private final List<TaskExecutorSpec> specs;
    private final LeaderSelector leaderSelector;

    volatile AtomicReference<CountDownLatch> debugLatch = new AtomicReference<>();

    public SchedulerSelector(WorkflowManagerImpl workflowManager, QueueFactory queueFactory, List<TaskExecutorSpec> specs)
    {
        this.workflowManager = workflowManager;
        this.queueFactory = queueFactory;
        this.specs = ImmutableList.copyOf(specs);

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

    private void takeLeadership()
    {
        log.info(workflowManager.getInstanceName() + " is now the scheduler");
        try
        {
            new Scheduler(workflowManager, queueFactory, specs).run();
        }
        finally
        {
            CountDownLatch latch = debugLatch.getAndSet(null);
            if ( latch != null )
            {
                latch.countDown();
            }
        }
    }
}
