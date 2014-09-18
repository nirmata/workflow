package com.nirmata.workflow;

import com.nirmata.workflow.models.ScheduleId;
import com.nirmata.workflow.models.TaskId;

public interface WorkflowManagerListener
{
    public void notifyScheduleStarted(ScheduleId scheduleId);

    public void notifyTaskExecuted(ScheduleId scheduleId, TaskId taskId);

    public void notifyScheduleCompleted(ScheduleId scheduleId);
}
