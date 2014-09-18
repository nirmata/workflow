package com.nirmata.workflow.details;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.nirmata.workflow.WorkflowManager;
import com.nirmata.workflow.details.internalmodels.DenormalizedWorkflowModel;
import com.nirmata.workflow.models.RunId;
import com.nirmata.workflow.models.ScheduleExecutionModel;
import com.nirmata.workflow.models.ScheduleId;
import com.nirmata.workflow.models.ScheduleModel;
import com.nirmata.workflow.models.TaskId;
import com.nirmata.workflow.models.TaskModel;
import com.nirmata.workflow.models.WorkflowModel;
import com.nirmata.workflow.spi.Clock;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static com.nirmata.workflow.details.InternalJsonSerializer.addDenormalizedWorkflow;
import static com.nirmata.workflow.spi.JsonSerializer.newNode;
import static com.nirmata.workflow.spi.JsonSerializer.toBytes;

public class Scheduler implements Closeable
{
    private final Logger log = LoggerFactory.getLogger(getClass());
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
        leaderSelector = new LeaderSelector(workflowManager.getCurator(), ZooKeeperConstants.getSchedulerLeaderPath(), listener);
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
                    if ( scheduleExecution == null )
                    {
                        scheduleExecution = new ScheduleExecutionModel(scheduleId, new Date(0), new Date(0), 0);
                    }
                    if ( schedule.shouldExecuteNow(scheduleExecution) )
                    {
                        startWorkflow(scheduleExecution, schedule, localStateCache);
                    }
                }
                else
                {
                    log.warn("Could not find schedule " + scheduleId);
                }
            }
        }
    }

    private void startWorkflow(ScheduleExecutionModel scheduleExecution, ScheduleModel schedule, StateCache localStateCache)
    {
        WorkflowModel workflow = localStateCache.getWorkflows().get(schedule.getWorkflowId());
        if ( workflow == null )
        {
            String message = "Expected workflow not found in StateCache. WorkflowId: " + schedule.getWorkflowId();
            log.error(message);
            throw new RuntimeException(message);
        }
        List<TaskModel> tasks = Lists.newArrayList();
        for ( List<TaskId> thisSet : workflow.getTasks() )
        {
            for ( TaskId taskId : thisSet )
            {
                TaskModel task = localStateCache.getTasks().get(taskId);
                if ( task == null )
                {
                    String message = "Expected task not found in StateCache. TaskId: " + taskId;
                    log.error(message);
                    throw new RuntimeException(message);
                }
                tasks.add(task);
            }
        }
        DenormalizedWorkflowModel denormalizedWorkflow = new DenormalizedWorkflowModel(new RunId(), scheduleExecution, workflow.getWorkflowId(), tasks, workflow.getName(), workflow.getTasks(), Clock.nowUtc(), 0);
        byte[] json = toJson(log, denormalizedWorkflow);

        try
        {
            workflowManager.getCurator().create().creatingParentsIfNeeded().forPath(ZooKeeperConstants.getRunPath(denormalizedWorkflow.getRunId()), json);
            log.info("Started workflow: " + schedule.getWorkflowId());
            workflowManager.notifyScheduleStarted(schedule.getScheduleId());
        }
        catch ( KeeperException.NodeExistsException ignore )
        {
            // happens due to cache latency
            log.debug("Workflow already started: " + workflow);
        }
        catch ( Exception e )
        {
            log.error("Could not create workflow node: " + workflow, e);
            throw new RuntimeException(e);
        }
    }

    static byte[] toJson(Logger log, DenormalizedWorkflowModel denormalizedWorkflow)
    {
        byte[] json = toBytes(addDenormalizedWorkflow(newNode(), denormalizedWorkflow));
        if ( json.length > ZooKeeperConstants.MAX_PAYLOAD )
        {
            String message = "JSON payload for workflow too big: " + denormalizedWorkflow;
            log.error(message);
            throw new RuntimeException(message);
        }
        return json;
    }

    private void takeLeadership()
    {
        Cacher cacher = new Cacher(workflowManager.getCurator(), new CacherListenerImpl(workflowManager));
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
            log.error("Exception while running scheduler", e);
        }
        finally
        {
            CloseableUtils.closeQuietly(cacher);
        }
    }
}
