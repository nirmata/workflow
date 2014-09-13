package com.nirmata.workflow.details;

import com.nirmata.workflow.models.TaskModel;
import com.nirmata.workflow.models.TaskSets;
import com.nirmata.workflow.models.WorkflowId;
import java.util.Date;
import java.util.List;

public class DenormalizedWorkflow
{
    private final WorkflowId workflowId;
    private final List<TaskModel> tasks;
    private final String name;
    private final TaskSets taskSets;
    private final Date startDateUtc;

    public DenormalizedWorkflow(WorkflowId workflowId, List<TaskModel> tasks, String name, TaskSets taskSets, Date startDateUtc)
    {
        this.workflowId = workflowId;
        this.tasks = tasks;
        this.name = name;
        this.taskSets = taskSets;
        this.startDateUtc = startDateUtc;
    }

    public WorkflowId getWorkflowId()
    {
        return workflowId;
    }

    public List<TaskModel> getTasks()
    {
        return tasks;
    }

    public String getName()
    {
        return name;
    }

    public TaskSets getTaskSets()
    {
        return taskSets;
    }

    public Date getStartDateUtc()
    {
        return startDateUtc;
    }
}
