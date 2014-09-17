package com.nirmata.workflow.details;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.nirmata.workflow.WorkflowManager;
import com.nirmata.workflow.details.internalmodels.DenormalizedWorkflowModel;
import com.nirmata.workflow.details.internalmodels.ExecutableTaskModel;
import com.nirmata.workflow.models.ScheduleExecutionModel;
import com.nirmata.workflow.models.TaskId;
import com.nirmata.workflow.models.TaskModel;
import com.nirmata.workflow.queue.Queue;
import com.nirmata.workflow.spi.Clock;
import org.apache.zookeeper.CreateMode;
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
        log.info("updateAndQueueTasks for " + workflow);
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

            if ( cacher.taskIsComplete(workflow.getScheduleId(), taskId) )
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
            DenormalizedWorkflowModel newWorkflow = new DenormalizedWorkflowModel(workflow.getScheduleExecution(), workflow.getWorkflowId(), workflow.getTasks(), workflow.getName(), workflow.getTaskSets(), workflow.getStartDateUtc(), taskSetsIndex + 1);
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
        log.info("Queueing task: " + task);
        String path = ZooKeeperConstants.getStartedTaskPath(workflow.getScheduleId(), task.getTaskId());
        try
        {
            workflowManager.getCurator().create().creatingParentsIfNeeded().forPath(path);
            Queue queue = task.isIdempotent() ? workflowManager.getIdempotentTaskQueue() : workflowManager.getNonIdempotentTaskQueue();
            queue.put(new ExecutableTaskModel(workflow.getScheduleId(), task));
        }
        catch ( KeeperException.NodeExistsException ignore )
        {
            log.info("Task already queued: " + task);
            // race - task already started
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
            String completedScheduleBasePath = ZooKeeperConstants.getCompletedScheduleBasePath(newWorkflow.getScheduleId());
            workflowManager.getCurator().create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath(completedScheduleBasePath, Scheduler.toJson(log, newWorkflow));

            workflowManager.getCurator().delete().guaranteed().inBackground().forPath(ZooKeeperConstants.getSchedulePath(newWorkflow.getScheduleId()));
            ScheduleExecutionModel scheduleExecution = new ScheduleExecutionModel(newWorkflow.getScheduleId(), newWorkflow.getStartDateUtc(), Clock.nowUtc(), newWorkflow.getScheduleExecution().getExecutionQty() + 1);
            workflowManager.getStorageBridge().updateScheduleExecution(scheduleExecution);
        }
        catch ( Exception e )
        {
            log.error("Could not completeSchedule for workflow: " + newWorkflow, e);
            throw new RuntimeException(e);
        }
    }
}
