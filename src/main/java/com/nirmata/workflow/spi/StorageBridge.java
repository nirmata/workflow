package com.nirmata.workflow.spi;

import com.nirmata.workflow.models.ScheduleExecutionModel;
import com.nirmata.workflow.models.ScheduleModel;
import com.nirmata.workflow.models.TaskModel;
import com.nirmata.workflow.models.WorkflowModel;
import java.util.List;

/**
 * Bridge between external storage and the workflow manager. This
 * bridge will be polled to get latest data.
 */
public interface StorageBridge
{
    /**
     * Return the current schedules
     *
     * @return schedules
     */
    public List<ScheduleModel> getScheduleModels();

    /**
     * Return the current workflows
     *
     * @return workflows
     */
    public List<WorkflowModel> getWorkflowModels();

    /**
     * Return the current tasks
     *
     * @return tasks
     */
    public List<TaskModel> getTaskModels();

    /**
     * Return the current executions
     *
     * @return executions
     */
    public List<ScheduleExecutionModel> getScheduleExecutions();

    /**
     * Update the value for an execution
     *
     * @param scheduleExecution execution to update
     */
    public void updateScheduleExecution(ScheduleExecutionModel scheduleExecution);
}
