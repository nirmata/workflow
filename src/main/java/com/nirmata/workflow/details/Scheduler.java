package com.nirmata.workflow.details;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.nirmata.workflow.WorkflowManager;
import com.nirmata.workflow.details.internalmodels.DenormalizedWorkflowModel;
import com.nirmata.workflow.models.ScheduleExecutionModel;
import com.nirmata.workflow.models.ScheduleId;
import com.nirmata.workflow.models.ScheduleModel;
import com.nirmata.workflow.models.TaskId;
import com.nirmata.workflow.models.TaskModel;
import com.nirmata.workflow.models.WorkflowModel;
import com.nirmata.workflow.spi.JsonSerializer;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.KeeperException;
import java.io.Closeable;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class Scheduler implements Closeable
{
    private final WorkflowManager workflowManager;
    private final LeaderSelector leaderSelector;
    private final AtomicReference<State> state = new AtomicReference<State>(State.LATENT);

    private enum State
    {
        LATENT,
        STARTED,
        CLOSED
    }

    public Scheduler(WorkflowManager workflowManager)
    {
        this.workflowManager = Preconditions.checkNotNull(workflowManager, "workflowManager cannot be null");
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
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "Already started");
        leaderSelector.start();
    }

    @Override
    public void close()
    {
        if ( state.compareAndSet(State.STARTED, State.CLOSED) )
        {
            leaderSelector.close();
        }
    }

    private void startNewTasks(Cacher cacher)
    {
        StateCache localStateCache = workflowManager.getStateCache();    // save local value so we're safe if master state cache changes

        for ( ScheduleId scheduleId : localStateCache.getSchedules().keySet() )
        {
            if ( !cacher.scheduleIsActive(scheduleId) )
            {
                ScheduleModel schedule = localStateCache.getSchedules().get(scheduleId);
                if ( schedule != null )
                {
                    ScheduleExecutionModel scheduleExecution = localStateCache.getScheduleExecutions().get(scheduleId);
                    if ( scheduleExecution != null )
                    {
                        if ( schedule.shouldExecuteNow(scheduleExecution) )
                        {
                            startWorkflow(schedule, localStateCache);
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

    private void startWorkflow(ScheduleModel schedule, StateCache localStateCache)
    {
        WorkflowModel workflow = localStateCache.getWorkflows().get(schedule.getWorkflowId());
        if ( workflow == null )
        {
            // TODO
        }
        List<TaskModel> tasks = Lists.newArrayList();
        for ( List<TaskId> thisSet : workflow.getTasks() )
        {
            ArrayNode tab = JsonSerializer.newArrayNode();
            for ( TaskId taskId : thisSet )
            {
                TaskModel task = localStateCache.getTasks().get(taskId);
                if ( task == null )
                {
                    // TODO
                }
                tasks.add(task);
            }
        }
        DenormalizedWorkflowModel denormalizedWorkflow = new DenormalizedWorkflowModel(schedule.getScheduleId(), workflow.getWorkflowId(), tasks, workflow.getName(), workflow.getTasks(), Clock.nowUtc(), 0);
        byte[] json = toJson(denormalizedWorkflow);

        try
        {
            workflowManager.getCurator().create().creatingParentsIfNeeded().forPath(ZooKeeperConstants.getScheduleKey(schedule.getScheduleId()), json);
        }
        catch ( KeeperException.NodeExistsException ignore )
        {
            // should never happen, but ignore in case it does
            // TODO log
        }
        catch ( Exception e )
        {
            // TODO
        }
    }

    static byte[] toJson(DenormalizedWorkflowModel denormalizedWorkflow)
    {
        byte[] json = JsonSerializer.toBytes(InternalJsonSerializer.addDenormalizedWorkflow(JsonSerializer.newNode(), denormalizedWorkflow));
        if ( json.length > ZooKeeperConstants.MAX_PAYLOAD )
        {
            // TODO
        }
        return json;
    }

    private void takeLeadership()
    {
        Cacher cacher = new Cacher(workflowManager, new CacherListenerImpl(workflowManager));
        try
        {
            cacher.start();
            while ( !Thread.currentThread().isInterrupted() )
            {
                Thread.sleep(workflowManager.getConfiguration().getSchedulerSleepMs());
                startNewTasks(cacher);
            }
        }
        catch ( InterruptedException dummy )
        {
            Thread.currentThread().interrupt();
        }
        catch ( Exception e )
        {
            // TODO log
        }
        finally
        {
            CloseableUtils.closeQuietly(cacher);
        }
    }
}
