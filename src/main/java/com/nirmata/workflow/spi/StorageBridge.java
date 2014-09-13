package com.nirmata.workflow.spi;

import com.nirmata.workflow.models.ScheduleExecutionModel;
import com.nirmata.workflow.models.ScheduleModel;
import com.nirmata.workflow.models.TaskModel;
import com.nirmata.workflow.models.WorkflowModel;
import java.util.List;

public interface StorageBridge
{
    public List<ScheduleModel> getScheduleModels();

    public List<WorkflowModel> getWorkflowModels();

    public List<TaskModel> getTaskModels();

    public List<ScheduleExecutionModel> getScheduleExecutions();

    public void updateScheduleExecution(ScheduleExecutionModel scheduleExecution);
}
