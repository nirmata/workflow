package com.nirmata.workflow.details;

import com.nirmata.workflow.WorkflowManager;
import com.nirmata.workflow.details.internalmodels.CompletedTaskModel;
import com.nirmata.workflow.details.internalmodels.ExecutableTaskModel;
import com.nirmata.workflow.spi.JsonSerializer;
import com.nirmata.workflow.spi.TaskExecution;
import com.nirmata.workflow.spi.TaskExecutionResult;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.queue.QueueConsumer;
import org.apache.curator.framework.state.ConnectionState;

public class TaskRunner implements QueueConsumer<ExecutableTaskModel>
{
    private final WorkflowManager workflowManager;

    public TaskRunner(WorkflowManager workflowManager)
    {
        this.workflowManager = workflowManager;
    }

    @Override
    public void consumeMessage(ExecutableTaskModel executableTask) throws Exception
    {
        TaskExecution taskExecution = workflowManager.getTaskExecutor().newTaskExecution(executableTask.getTask());
        TaskExecutionResult result = taskExecution.execute();
        CompletedTaskModel completedTask = new CompletedTaskModel(true, result.getResultData());
        String json = JsonSerializer.toString(InternalJsonSerializer.addCompletedTask(JsonSerializer.newNode(), completedTask));
        String path = ZooKeeperConstants.getCompletedTaskKey(executableTask.getScheduleId(), executableTask.getTask().getTaskId());
        workflowManager.getCurator().create().creatingParentsIfNeeded().forPath(path, json.getBytes());
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState)
    {
        // NOP - let other parts of the app handle it
    }
}
