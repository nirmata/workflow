package com.nirmata.workflow.admin;

import com.google.common.collect.ImmutableMap;
import com.nirmata.workflow.details.WorkflowStatus;
import com.nirmata.workflow.details.ZooKeeperConstants;
import com.nirmata.workflow.details.internalmodels.DenormalizedWorkflowModel;
import com.nirmata.workflow.models.RunId;
import com.nirmata.workflow.models.StartedTaskModel;
import com.nirmata.workflow.models.TaskId;
import com.nirmata.workflow.spi.TaskExecutionResult;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.nirmata.workflow.details.InternalJsonSerializer.getDenormalizedWorkflow;
import static com.nirmata.workflow.spi.JsonSerializer.*;

public class RunReport
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final DenormalizedWorkflowModel workflow;
    private final Map<TaskId, TaskExecutionResult> completedTasks;
    private final Map<TaskId, StartedTaskModel> runningTasks;
    private final RunId runId;

    public RunReport(CuratorFramework curator, RunId runId)
    {
        this.runId = runId;
        ImmutableMap.Builder<TaskId, TaskExecutionResult> completedTasksBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<TaskId, StartedTaskModel> runningTasksBuilder = ImmutableMap.builder();

        DenormalizedWorkflowModel localWorkflow = loadRunning(curator, runId);
        if ( localWorkflow == null )
        {
            localWorkflow = loadCompleted(curator, runId);
        }

        workflow = localWorkflow;
        if ( isValid() )
        {
            getCompletedTasks(curator, runId, completedTasksBuilder);
        }

        completedTasks = completedTasksBuilder.build();
        if ( isValid() )
        {
            getRunningTasks(curator, runId, runningTasksBuilder, completedTasks.keySet());
        }
        runningTasks = runningTasksBuilder.build();
    }

    public LocalDateTime getStartDateUtc()
    {
        return (workflow != null) ? workflow.getStartDateUtc() : null;
    }

    public Map<TaskId, TaskExecutionResult> getCompletedTasks()
    {
        return completedTasks;
    }

    public Map<TaskId, StartedTaskModel> getRunningTasks()
    {
        return runningTasks;
    }

    public boolean isValid()
    {
        return (workflow != null);
    }

    public WorkflowStatus getStatus()
    {
        return isValid() ? workflow.getStatus() : null;
    }

    public RunId getRunId()
    {
        return runId;
    }

    private void getRunningTasks(CuratorFramework curator, RunId runId, ImmutableMap.Builder<TaskId, StartedTaskModel> builder, Collection<TaskId> completedTaskIds)
    {
        String path = ZooKeeperConstants.getStartedTasksParentPath();
        try
        {
            List<String> children = curator.getChildren().forPath(path);
            for ( String name : children )
            {
                RunId thisRunId = new RunId(ZooKeeperConstants.getRunIdFromCompletedTasksPath(ZKPaths.makePath(path, name)));
                if ( thisRunId.equals(runId) )
                {
                    TaskId taskId = new TaskId(ZooKeeperConstants.getTaskIdFromCompletedTasksPath(ZKPaths.makePath(path, name)));
                    if ( !completedTaskIds.contains(taskId) )
                    {
                        byte[] bytes = curator.getData().forPath(ZooKeeperConstants.getStartedTaskPath(runId, taskId));
                        StartedTaskModel result = getStartedTask(fromBytes(bytes));
                        builder.put(taskId, result);
                    }
                }
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
        String path = ZooKeeperConstants.getCompletedTasksParentPath();
        try
        {
            List<String> children = curator.getChildren().forPath(path);
            for ( String name : children )
            {
                RunId thisRunId = new RunId(ZooKeeperConstants.getRunIdFromCompletedTasksPath(ZKPaths.makePath(path, name)));
                if ( thisRunId.equals(runId) )
                {
                    TaskId taskId = new TaskId(ZooKeeperConstants.getTaskIdFromCompletedTasksPath(ZKPaths.makePath(path, name)));
                    byte[] bytes = curator.getData().forPath(ZooKeeperConstants.getCompletedTaskPath(runId, taskId));
                    TaskExecutionResult result = getTaskExecutionResult(fromBytes(bytes));
                    builder.put(taskId, result);
                }
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

    private DenormalizedWorkflowModel loadCompleted(CuratorFramework curator, RunId runId)
    {
        String completedRunPath = ZooKeeperConstants.getCompletedRunPath(runId);
        return getDenormalizedWorkflowModel(curator, runId, completedRunPath);
    }

    private DenormalizedWorkflowModel loadRunning(CuratorFramework curator, RunId runId)
    {
        String runPath = ZooKeeperConstants.getRunPath(runId);
        return getDenormalizedWorkflowModel(curator, runId, runPath);
    }

    private DenormalizedWorkflowModel getDenormalizedWorkflowModel(CuratorFramework curator, RunId runId, String runPath)
    {
        try
        {
            byte[] bytes = curator.getData().forPath(runPath);
            return getDenormalizedWorkflow(fromBytes(bytes));
        }
        catch ( KeeperException.NoNodeException dummy )
        {
            // ignore
        }
        catch ( Exception e )
        {
            log.error("Could not load workflow for run: " + runId, e);
            throw new RuntimeException(e);
        }
        return null;
    }
}
