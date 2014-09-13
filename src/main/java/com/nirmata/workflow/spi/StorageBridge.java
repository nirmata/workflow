package com.nirmata.workflow.spi;

import com.nirmata.workflow.models.ScheduleId;
import com.nirmata.workflow.models.ScheduleModel;
import com.nirmata.workflow.models.TaskModel;
import com.nirmata.workflow.models.WorkflowModel;
import java.util.Date;
import java.util.List;

public interface StorageBridge
{
    public List<ScheduleModel> getScheduleModels();

    public List<WorkflowModel> getWorkflowModels();

    public List<TaskModel> getTaskModels();

    public void updateScheduleLastExecution(ScheduleId scheduleId, Date lastExecution);
}
