package com.nirmata.workflow.details;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.nirmata.workflow.WorkflowManager;
import com.nirmata.workflow.details.internalmodels.CompletedTaskModel;
import com.nirmata.workflow.details.internalmodels.DenormalizedWorkflowModel;
import com.nirmata.workflow.details.internalmodels.ExecutableTaskModel;
import com.nirmata.workflow.models.ScheduleExecutionModel;
import com.nirmata.workflow.models.ScheduleId;
import com.nirmata.workflow.models.TaskId;
import com.nirmata.workflow.models.TaskModel;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;

import static com.nirmata.workflow.details.InternalJsonSerializer.getCompletedTask;
import static com.nirmata.workflow.spi.JsonSerializer.*;

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

            ChildData currentData = cacher.getCompletedData(workflow.getScheduleId(), taskId);
            if ( currentData != null )
            {
                CompletedTaskModel completedTask = getCompletedTask(fromBytes(currentData.getData()));
                if ( completedTask.isComplete() )
                {
                    ++completedQty;
                }
                else
                {
                    // TODO requeue?
                }
            }
            else
            {
                queueTask(workflow.getScheduleId(), task);
            }
        }

        if ( completedQty == thisTasks.size() )
        {
            DenormalizedWorkflowModel newWorkflow = new DenormalizedWorkflowModel(workflow.getScheduleExecution(), workflow.getWorkflowId(), workflow.getTasks(), workflow.getName(), workflow.getTaskSets(), workflow.getStartDateUtc(), taskSetsIndex + 1);
            try
            {
                if ( newWorkflow.getTaskSetsIndex() >= workflow.getTaskSets().size() )
                {
                    completeSchedule(newWorkflow);
                }
                else
                {
                    workflowManager.getCurator().setData().forPath(ZooKeeperConstants.getSchedulePath(newWorkflow.getScheduleId()), Scheduler.toJson(log, newWorkflow));
                    updateAndQueueTasks(cacher, newWorkflow);
                }
            }
            catch ( Exception e )
            {
                log.error("Could not create paths for completed workflow: ", workflow, e);
                throw new RuntimeException(e);
            }
        }
    }

    private void completeSchedule(DenormalizedWorkflowModel newWorkflow) throws Exception
    {
        workflowManager.getCurator().create().creatingParentsIfNeeded().forPath(ZooKeeperConstants.getCompletedSchedulePath(newWorkflow.getScheduleId()), Scheduler.toJson(log, newWorkflow));
        workflowManager.getCurator().delete().guaranteed().inBackground().forPath(ZooKeeperConstants.getSchedulePath(newWorkflow.getScheduleId()));
        ScheduleExecutionModel scheduleExecution = new ScheduleExecutionModel(newWorkflow.getScheduleId(), newWorkflow.getStartDateUtc(), Clock.nowUtc(), newWorkflow.getScheduleExecution().getExecutionQty() + 1);
        workflowManager.getStorageBridge().updateScheduleExecution(scheduleExecution);
    }

    private void queueTask(ScheduleId scheduleId, TaskModel task)
    {
        String path = ZooKeeperConstants.getCompletedTaskPath(scheduleId, task.getTaskId());
        ObjectNode node = InternalJsonSerializer.addCompletedTask(newNode(), new CompletedTaskModel());
        byte[] json = toBytes(node);
        try
        {
            workflowManager.getCurator().create().creatingParentsIfNeeded().forPath(path, json);
            workflowManager.executeTask(new ExecutableTaskModel(scheduleId, task));
        }
        catch ( KeeperException.NodeExistsException ignore )
        {
            // already exists - just ignore
        }
        catch ( Exception e )
        {
            log.error(String.format("Could not queue tasks for schedule (%s) and task (%s)", scheduleId, task), e);
            throw new RuntimeException(e);
        }
    }
}
