package com.nirmata.workflow.details;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.nirmata.workflow.WorkflowManager;
import com.nirmata.workflow.models.RunId;
import com.nirmata.workflow.models.Task;
import com.nirmata.workflow.models.TaskId;
import com.nirmata.workflow.queue.QueueFactory;
import org.apache.curator.framework.CuratorFramework;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class WorkflowManagerImpl implements WorkflowManager
{
    private final CuratorFramework curator;
    private final QueueFactory queueFactory;
    private final String instanceName;
    private final List<TaskExecutorSpec> specs;

    public WorkflowManagerImpl(CuratorFramework curator, QueueFactory queueFactory, String instanceName, List<TaskExecutorSpec> specs)
    {
        this.curator = Preconditions.checkNotNull(curator, "curator cannot be null");
        this.queueFactory = Preconditions.checkNotNull(queueFactory, "queueFactory cannot be null");
        this.instanceName = Preconditions.checkNotNull(instanceName, "instanceName cannot be null");
        specs = Preconditions.checkNotNull(specs, "specs cannot be null");
        this.specs = ImmutableList.copyOf(specs);
    }

    public CuratorFramework getCurator()
    {
        return curator;
    }

    @Override
    public void start()
    {

    }

    @Override
    public RunId submitTask(Task task)
    {
        return null;
    }

    @Override
    public RunId submitSubTask(Task task, RunId mainRunId, TaskId mainTaskId)
    {
        return null;
    }

    @Override
    public Map<String, String> getTaskData(RunId runId, TaskId taskId)
    {
        return null;
    }

    @Override
    public void close() throws IOException
    {

    }

    public void executeTask(ExecutableTaskModel executableTask)
    {

    }
}
