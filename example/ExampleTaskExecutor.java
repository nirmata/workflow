import com.nirmata.workflow.executor.TaskExecution;
import com.nirmata.workflow.executor.TaskExecutionStatus;
import com.nirmata.workflow.executor.TaskExecutor;
import com.nirmata.workflow.models.ExecutableTask;
import com.nirmata.workflow.models.TaskExecutionResult;

public class ExampleTaskExecutor implements TaskExecutor
{
    @Override
    public TaskExecution newTaskExecution(WorkflowManager workflowManager, ExecutableTask executableTask)
    {
        return new TaskExecution()
        {
            @Override
            public TaskExecutionResult execute()
            {
                System.out.println("Executing task: " + executableTask.getTaskId());
                return new TaskExecutionResult(TaskExecutionStatus.SUCCESS, "My message");
            }
        };
    }
}
