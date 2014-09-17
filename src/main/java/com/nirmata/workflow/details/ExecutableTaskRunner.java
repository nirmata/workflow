package com.nirmata.workflow.details;

import com.google.common.base.Preconditions;
import com.nirmata.workflow.WorkflowManager;
import com.nirmata.workflow.details.internalmodels.CompletedTaskModel;
import com.nirmata.workflow.details.internalmodels.ExecutableTaskModel;
import com.nirmata.workflow.spi.TaskExecution;
import com.nirmata.workflow.spi.TaskExecutionResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.nirmata.workflow.details.InternalJsonSerializer.*;
import static com.nirmata.workflow.spi.JsonSerializer.*;

public class ExecutableTaskRunner
{
    private final Logger log = LoggerFactory.getLogger(getClass());
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
        String json = nodeToString(addCompletedTask(newNode(), completedTask));
        try
        {
            String path = ZooKeeperConstants.getCompletedTaskPath(executableTask.getScheduleId(), executableTask.getTask().getTaskId());
            workflowManager.getCurator().setData().forPath(path, json.getBytes());
        }
        catch ( Exception e )
        {
            log.error("Could not set completed data for executable task: " + executableTask, e);
            throw new RuntimeException(e);
        }
    }
}
