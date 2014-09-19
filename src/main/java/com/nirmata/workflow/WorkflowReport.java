package com.nirmata.workflow;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.nirmata.workflow.details.ZooKeeperConstants;
import com.nirmata.workflow.details.internalmodels.DenormalizedWorkflowModel;
import com.nirmata.workflow.details.internalmodels.StartedTaskModel;
import com.nirmata.workflow.models.RunId;
import com.nirmata.workflow.models.TaskId;
import com.nirmata.workflow.spi.TaskExecutionResult;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static com.nirmata.workflow.details.InternalJsonSerializer.*;
import static com.nirmata.workflow.spi.JsonSerializer.*;

public class WorkflowReport
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final DenormalizedWorkflowModel workflow;
    private final Map<TaskId, TaskExecutionResult> completedTasks;
    private final Map<TaskId, Date> runningTasks;

    public WorkflowReport(CuratorFramework curator, RunId runId)
    {
        ImmutableMap.Builder<TaskId, TaskExecutionResult> completedTasksBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<TaskId, Date> runningTasksBuilder = ImmutableMap.builder();

        workflow = init(curator, runId);
        if ( isValid() )
        {
            getCompletedTasks(curator, runId, completedTasksBuilder);
            getRunningTasks(curator, runId, runningTasksBuilder);
        }

        completedTasks = completedTasksBuilder.build();
        runningTasks = runningTasksBuilder.build();
    }

    public Date getStartDateUtc()
    {
        Preconditions.checkState(isValid(), "report is not valid");
        return workflow.getStartDateUtc();
    }

    public Map<TaskId, TaskExecutionResult> getCompletedTasks()
    {
        Preconditions.checkState(isValid(), "report is not valid");
        return completedTasks;
    }

    public Map<TaskId, Date> getRunningTasks()
    {
        Preconditions.checkState(isValid(), "report is not valid");
        return runningTasks;
    }

    public boolean isValid()
    {
        return (workflow != null);
    }

    private void getRunningTasks(CuratorFramework curator, RunId runId, ImmutableMap.Builder<TaskId, Date> builder)
    {
        String path = ZooKeeperConstants.getStartedTasksParentPath(runId);
        try
        {
            List<String> children = curator.getChildren().forPath(path);
            for ( String taskIdStr : children )
            {
                TaskId taskId = new TaskId(taskIdStr);
                byte[] bytes = curator.getData().forPath(ZooKeeperConstants.getStartedTaskPath(runId, taskId));
                StartedTaskModel result = getStartedTask(fromBytes(bytes));
                builder.put(taskId, result.getStartDateUtc());
            }
        }
        catch ( KeeperException.NoNodeException dummy )
        {
            // just ignore - no running tasks
        }
        catch ( Exception e )
        {
            log.error("Could not build running tasks for run: " + runId, e);
            throw new RuntimeException(e);
        }
    }

    private void getCompletedTasks(CuratorFramework curator, RunId runId, ImmutableMap.Builder<TaskId, TaskExecutionResult> builder)
    {
        String path = ZooKeeperConstants.getCompletedTasksParentPath(runId);
        try
        {
            List<String> children = curator.getChildren().forPath(path);
            for ( String taskIdStr : children )
            {
                TaskId taskId = new TaskId(taskIdStr);
                byte[] bytes = curator.getData().forPath(ZooKeeperConstants.getCompletedTaskPath(runId, taskId));
                TaskExecutionResult result = getTaskExecutionResult(fromBytes(bytes));
                builder.put(taskId, result);
            }
        }
        catch ( KeeperException.NoNodeException dummy )
        {
            // just ignore - no completed tasks
        }
        catch ( Exception e )
        {
            log.error("Could not build completed tasks for run: " + runId, e);
            throw new RuntimeException(e);
        }
    }

    private DenormalizedWorkflowModel init(CuratorFramework curator, RunId runId)
    {
        String runPath = ZooKeeperConstants.getRunPath(runId);
        try
        {
            byte[] bytes = curator.getData().forPath(runPath);
            return getDenormalizedWorkflow(fromBytes(bytes));
        }
        catch ( KeeperException.NodeExistsException dummy )
        {
            // puts report in invalid state
        }
        catch ( Exception e )
        {
            log.error("Could not load workflow for run: " + runId, e);
            throw new RuntimeException(e);
        }
        return null;
    }
}
