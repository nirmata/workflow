package com.nirmata.workflow;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;
import com.nirmata.workflow.models.ScheduleExecutionModel;
import com.nirmata.workflow.models.ScheduleId;
import com.nirmata.workflow.models.ScheduleModel;
import com.nirmata.workflow.models.TaskDagContainerModel;
import com.nirmata.workflow.models.TaskId;
import com.nirmata.workflow.models.TaskModel;
import com.nirmata.workflow.models.WorkflowModel;
import com.nirmata.workflow.spi.StorageBridge;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.nirmata.workflow.spi.JsonSerializer.*;

class MockStorageBridge implements StorageBridge
{
    private final List<ScheduleModel> schedules;
    private final List<WorkflowModel> workflows;
    private final Map<TaskId, TaskModel> tasks;
    private final List<TaskDagContainerModel> taskContainers;
    private final Map<ScheduleId, ScheduleExecutionModel> scheduleExecutions;

    public MockStorageBridge(String schedulesFile, String tasksFile, String workflowsFile, String taskDagContainersFile) throws IOException
    {
        scheduleExecutions = Maps.newHashMap();
        schedules = getSchedules(fromString(Resources.toString(Resources.getResource(schedulesFile), Charset.defaultCharset())));
        tasks = getTasks(fromString(Resources.toString(Resources.getResource(tasksFile), Charset.defaultCharset()))).stream().collect(Collectors.toMap(TaskModel::getTaskId, Function.<TaskModel>identity()));
        workflows = getWorkflows(fromString(Resources.toString(Resources.getResource(workflowsFile), Charset.defaultCharset())));
        taskContainers = getTaskDagContainers(fromString(Resources.toString(Resources.getResource(taskDagContainersFile), Charset.defaultCharset())));
    }

    @Override
    public List<ScheduleModel> getScheduleModels()
    {
        return schedules;
    }

    @Override
    public List<WorkflowModel> getWorkflowModels()
    {
        return workflows;
    }

    @Override
    public List<TaskModel> getTaskModels()
    {
        return ImmutableList.copyOf(tasks.values());
    }

    @Override
    public List<TaskDagContainerModel> getTaskDagContainerModels()
    {
        return taskContainers;
    }

    @Override
    public List<ScheduleExecutionModel> getScheduleExecutions()
    {
        return Lists.newArrayList(scheduleExecutions.values());
    }

    @Override
    public void updateScheduleExecution(ScheduleExecutionModel scheduleExecution)
    {
        scheduleExecutions.put(scheduleExecution.getScheduleId(), scheduleExecution);
    }
}
