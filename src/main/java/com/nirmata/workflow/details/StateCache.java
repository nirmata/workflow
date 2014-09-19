package com.nirmata.workflow.details;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.nirmata.workflow.models.ScheduleExecutionModel;
import com.nirmata.workflow.models.ScheduleId;
import com.nirmata.workflow.models.ScheduleModel;
import com.nirmata.workflow.models.TaskId;
import com.nirmata.workflow.models.TaskModel;
import com.nirmata.workflow.models.WorkflowId;
import com.nirmata.workflow.models.WorkflowModel;
import java.util.List;
import java.util.Map;

public class StateCache
{
    private final Map<ScheduleId, ScheduleModel> schedules;
    private final Map<ScheduleId, ScheduleExecutionModel> scheduleExecutions;
    private final Map<WorkflowId, WorkflowModel> workflows;
    private final Map<TaskId, TaskModel> tasks;

    public StateCache(List<ScheduleModel> schedules, List<ScheduleExecutionModel> scheduleExecutions, List<TaskModel> tasks, List<WorkflowModel> workflows)
    {
        schedules = Preconditions.checkNotNull(schedules, "schedules cannot be null");
        scheduleExecutions = Preconditions.checkNotNull(scheduleExecutions, "scheduleExecutions cannot be null");
        tasks = Preconditions.checkNotNull(tasks, "tasks cannot be null");
        workflows = Preconditions.checkNotNull(workflows, "workflows cannot be null");

        this.schedules = Maps.uniqueIndex(schedules, ScheduleModel::getScheduleId);
        this.scheduleExecutions = Maps.uniqueIndex(scheduleExecutions, ScheduleExecutionModel::getScheduleId);
        this.tasks = Maps.uniqueIndex(tasks, TaskModel::getTaskId);
        this.workflows = Maps.uniqueIndex(workflows, WorkflowModel::getWorkflowId);
    }

    public StateCache()
    {
        this.schedules = ImmutableMap.of();
        this.scheduleExecutions = ImmutableMap.of();
        this.workflows = ImmutableMap.of();
        this.tasks = ImmutableMap.of();
    }

    public Map<ScheduleId, ScheduleModel> getSchedules()
    {
        return schedules;
    }

    public Map<WorkflowId, WorkflowModel> getWorkflows()
    {
        return workflows;
    }

    public Map<TaskId, TaskModel> getTasks()
    {
        return tasks;
    }

    public Map<ScheduleId, ScheduleExecutionModel> getScheduleExecutions()
    {
        return scheduleExecutions;
    }
}
