package com.nirmata.workflow.details;

import com.google.common.base.Function;
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

    private static final Function<? super ScheduleModel, ScheduleId> scheduleIdFunction = new Function<ScheduleModel, ScheduleId>()
    {
        @Override
        public ScheduleId apply(ScheduleModel scheduleModel)
        {
            return scheduleModel.getScheduleId();
        }
    };
    private static final Function<? super ScheduleExecutionModel, ScheduleId> scheduleExecutionIdFunction = new Function<ScheduleExecutionModel, ScheduleId>()
    {
        @Override
        public ScheduleId apply(ScheduleExecutionModel scheduleExecution)
        {
            return scheduleExecution.getScheduleId();
        }
    };
    public static final Function<? super TaskModel, TaskId> taskIdFunction = new Function<TaskModel, TaskId>()
    {
        @Override
        public TaskId apply(TaskModel taskModel)
        {
            return taskModel.getTaskId();
        }
    };
    private static final Function<? super WorkflowModel, WorkflowId> workflowIdFunction = new Function<WorkflowModel, WorkflowId>()
    {
        @Override
        public WorkflowId apply(WorkflowModel workflowModel)
        {
            return workflowModel.getWorkflowId();
        }
    };

    public StateCache(List<ScheduleModel> schedules, List<ScheduleExecutionModel> scheduleExecutions, List<TaskModel> tasks, List<WorkflowModel> workflows)
    {
        schedules = Preconditions.checkNotNull(schedules, "schedules cannot be null");
        scheduleExecutions = Preconditions.checkNotNull(scheduleExecutions, "scheduleExecutions cannot be null");
        tasks = Preconditions.checkNotNull(tasks, "tasks cannot be null");
        workflows = Preconditions.checkNotNull(workflows, "workflows cannot be null");

        this.schedules = Maps.uniqueIndex(schedules, scheduleIdFunction);
        this.scheduleExecutions = Maps.uniqueIndex(scheduleExecutions, scheduleExecutionIdFunction);
        this.tasks = Maps.uniqueIndex(tasks, taskIdFunction);
        this.workflows = Maps.uniqueIndex(workflows, workflowIdFunction);
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
