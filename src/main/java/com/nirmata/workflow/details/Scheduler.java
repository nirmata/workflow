package com.nirmata.workflow.details;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.nirmata.workflow.WorkflowManager;
import com.nirmata.workflow.models.ScheduleExecutionModel;
import com.nirmata.workflow.models.ScheduleId;
import com.nirmata.workflow.models.ScheduleModel;
import com.nirmata.workflow.spi.JsonSerializer;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.utils.CloseableUtils;
import java.io.Closeable;

public class Scheduler implements Closeable
{
    private final WorkflowManager workflowManager;
    private final LeaderSelector leaderSelector;

    public Scheduler(WorkflowManager workflowManager)
    {
        this.workflowManager = workflowManager;
        LeaderSelectorListener listener = new LeaderSelectorListenerAdapter()
        {
            @Override
            public void takeLeadership(CuratorFramework client) throws Exception
            {
                Scheduler.this.takeLeadership();
            }
        };
        leaderSelector = new LeaderSelector(workflowManager.getCurator(), ZooKeeperConstants.SCHEDULER_LEADER_PATH, listener);
        leaderSelector.autoRequeue();
    }

    public void start()
    {
        leaderSelector.start();
    }

    @Override
    public void close()
    {
        leaderSelector.close();
    }

    private void monitorRunningTasks()
    {

    }

    private void startNewTasks()
    {
        PathChildrenCache schedulesCache = workflowManager.getSchedulesCache();
        StateCache stateCache = workflowManager.getStateCache();    // save local value so we're safe if master state cache changes
        for ( ScheduleId scheduleId : stateCache.getSchedules().keySet() )
        {
            if ( schedulesCache.getCurrentData(ZooKeeperConstants.getScheduleKey(scheduleId)) == null )
            {
                ScheduleModel schedule = stateCache.getSchedules().get(scheduleId);
                if ( schedule != null )
                {
                    ScheduleExecutionModel scheduleExecution = stateCache.getScheduleExecutions().get(scheduleId);
                    if ( scheduleExecution != null )
                    {
                        if ( schedule.shouldExecuteNow(scheduleExecution) )
                        {
                            startWorkflow(schedule, stateCache);
                        }
                    }
                    else
                    {
                        // TODO logging needed?
                    }
                }
                else
                {
                    // TODO logging needed?
                }
            }
        }
    }

    private void startWorkflow(ScheduleModel schedule, StateCache stateCache)
    {
        ObjectNode node = JsonSerializer.newNode();
        InternalJsonSerializer.addDenormalizedWorkflow(node, stateCache, schedule.getWorkflowId());
        String json = JsonSerializer.toString(node);
        if ( json.length() > ZooKeeperConstants.MAX_PAYLOAD )
        {
            // TODO
        }

        try
        {
            workflowManager.getCurator().create().creatingParentsIfNeeded().forPath(ZooKeeperConstants.getScheduleKey(schedule.getScheduleId()), json.getBytes());
        }
        catch ( Exception e )
        {
            // TODO
        }
    }

    private void takeLeadership()
    {
        try
        {
            while ( !Thread.currentThread().isInterrupted() )
            {
                Thread.sleep(workflowManager.getConfiguration().getSchedulerSleepMs());
                monitorRunningTasks();
                startNewTasks();
            }
        }
        catch ( InterruptedException dummy )
        {
            Thread.currentThread().interrupt();
        }
    }
}
