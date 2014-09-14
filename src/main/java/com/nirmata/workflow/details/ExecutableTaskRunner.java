package com.nirmata.workflow.details;

import com.google.common.base.Preconditions;
import com.nirmata.workflow.WorkflowManager;
import com.nirmata.workflow.details.internalmodels.CompletedTaskModel;
import com.nirmata.workflow.details.internalmodels.ExecutableTaskModel;
import com.nirmata.workflow.spi.JsonSerializer;
import com.nirmata.workflow.spi.TaskExecution;
import com.nirmata.workflow.spi.TaskExecutionResult;

public class ExecutableTaskRunner
{
    private final WorkflowManager workflowManager;

    public ExecutableTaskRunner(WorkflowManager workflowManager)
    {
        this.workflowManager = Preconditions.checkNotNull(workflowManager, "workflowManager cannot be null");
    }

    public void executeTask(ExecutableTaskModel executableTask)
    {
        TaskExecution taskExecution = workflowManager.getTaskExecutor().newTaskExecution(executableTask.getTask());

        TaskExecutionResult result = taskExecution.execute();
        CompletedTaskModel completedTask = new CompletedTaskModel(true, result.getResultData());
        String json = JsonSerializer.toString(InternalJsonSerializer.addCompletedTask(JsonSerializer.newNode(), completedTask));
        try
        {
            String path = ZooKeeperConstants.getCompletedTaskKey(executableTask.getScheduleId(), executableTask.getTask().getTaskId());
            workflowManager.getCurator().setData().forPath(path, json.getBytes());
        }
        catch ( Exception e )
        {
            // TODO log
            throw new RuntimeException(e);
        }
    }
}
