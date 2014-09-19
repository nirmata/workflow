package com.nirmata.workflow.details;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.nirmata.workflow.WorkflowManager;
import com.nirmata.workflow.details.internalmodels.DenormalizedWorkflowModel;
import com.nirmata.workflow.details.internalmodels.StartedTaskModel;
import com.nirmata.workflow.models.ExecutableTaskModel;
import com.nirmata.workflow.models.ScheduleExecutionModel;
import com.nirmata.workflow.models.TaskId;
import com.nirmata.workflow.models.TaskModel;
import com.nirmata.workflow.queue.Queue;
import com.nirmata.workflow.spi.Clock;
import com.nirmata.workflow.spi.JsonSerializer;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;

class CacherListenerImpl implements CacherListener
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final WorkflowManager workflowManager;

    CacherListenerImpl(WorkflowManager workflowManager)
    {
        this.workflowManager = workflowManager;
    }

    @Override
    public void updateAndQueueTasks(Cacher cacher, DenormalizedWorkflowModel workflow)
    {
        ImmutableMap<TaskId, TaskModel> tasks = Maps.uniqueIndex(workflow.getTasks(), StateCache.taskIdFunction);
        int taskSetsIndex = workflow.getTaskSetsIndex();
        int completedQty = 0;
        List<TaskId> thisTasks = workflow.getTaskSets().get(taskSetsIndex);
        for ( TaskId taskId : thisTasks )
        {
            TaskModel task = tasks.get(taskId);
            if ( task == null )
            {
                String message = "Expected task not found in workflow " + taskId;
                log.error(message);
                throw new RuntimeException(message);
            }

            if ( cacher.taskIsComplete(workflow.getRunId(), taskId) )
            {
                ++completedQty;
            }
            else
            {
                queueTask(workflow, task);
            }
        }

        if ( completedQty == thisTasks.size() )
        {
            DenormalizedWorkflowModel newWorkflow = new DenormalizedWorkflowModel(workflow.getRunId(), workflow.getScheduleExecution(), workflow.getWorkflowId(), workflow.getTasks(), workflow.getName(), workflow.getTaskSets(), workflow.getStartDateUtc(), taskSetsIndex + 1);
            if ( newWorkflow.getTaskSetsIndex() >= workflow.getTaskSets().size() )
            {
                completeSchedule(newWorkflow);
            }
            else
            {
                cacher.updateSchedule(newWorkflow);
            }
        }
    }

    private void queueTask(DenormalizedWorkflowModel workflow, TaskModel task)
    {
        String path = ZooKeeperConstants.getStartedTaskPath(workflow.getRunId(), task.getTaskId());
        try
        {
            byte[] data = JsonSerializer.toBytes(InternalJsonSerializer.addStartedTask(JsonSerializer.newNode(), new StartedTaskModel(Clock.nowUtc())));
            workflowManager.getCurator().create().creatingParentsIfNeeded().forPath(path, data);
            Queue queue = task.isIdempotent() ? workflowManager.getIdempotentTaskQueue() : workflowManager.getNonIdempotentTaskQueue();
            queue.put(new ExecutableTaskModel(workflow.getRunId(), workflow.getScheduleId(), task));
            log.info("Queued task: " + task);
        }
        catch ( KeeperException.NodeExistsException ignore )
        {
            log.debug("Task already queued: " + task);
            // race due to caching latency - task already started
        }
        catch ( Exception e )
        {
            String message = "Could not start task " + task;
            log.error(message, e);
            throw new RuntimeException(e);
        }
    }

    private void completeSchedule(DenormalizedWorkflowModel newWorkflow)
    {
        try
        {
            log.info("Completing workflow: " + newWorkflow);
            String completedBasePath = ZooKeeperConstants.getCompletedRunPath(newWorkflow.getRunId());
            workflowManager.getCurator().create().creatingParentsIfNeeded().forPath(completedBasePath, Scheduler.toJson(log, newWorkflow));

            workflowManager.getCurator().delete().guaranteed().inBackground().forPath(ZooKeeperConstants.getRunPath(newWorkflow.getRunId()));
            ScheduleExecutionModel scheduleExecution = new ScheduleExecutionModel(newWorkflow.getScheduleId(), newWorkflow.getStartDateUtc(), Clock.nowUtc(), newWorkflow.getScheduleExecution().getExecutionQty() + 1);
            workflowManager.getStorageBridge().updateScheduleExecution(scheduleExecution);

            workflowManager.notifyScheduleCompleted(newWorkflow.getScheduleId());
        }
        catch ( Exception e )
        {
            log.error("Could not completeSchedule for workflow: " + newWorkflow, e);
            throw new RuntimeException(e);
        }
    }
}
