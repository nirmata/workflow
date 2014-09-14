package com.nirmata.workflow.details;

import com.nirmata.workflow.models.ScheduleId;
import com.nirmata.workflow.models.TaskId;
import org.apache.curator.utils.ZKPaths;

public class ZooKeeperConstants
{
    public static final String SCHEDULER_LEADER_PATH = "/scheduler-leader";
    public static final String SCHEDULES_PATH = "/schedules";
    public static final String TASK_LOCKS_PATH = "/task-locks";
    public static final String COMPLETED_TASKS_PATH = "/tasks-completed";
    public static final String IDEMPOTENT_TASKS_QUEUE_PATH = "/tasks-queue";
    public static final String NON_IDEMPOTENT_TASKS_QUEUE_PATH = "/tasks-queue-non";
    public static final String IDEMPOTENT_TASKS_QUEUE_LOCK_PATH = "/tasks-queue-locks";

    public static final int MAX_PAYLOAD = 0xfffff;  // see "jute.maxbuffer" at http://zookeeper.apache.org/doc/r3.3.1/zookeeperAdmin.html

    private static final String SEPARATOR = "|";

    private ZooKeeperConstants()
    {
    }

    public static String getScheduleKey(ScheduleId scheduleId)
    {
        return ZKPaths.makePath(SCHEDULES_PATH, scheduleId.getId());
    }

    public static String getTaskLockKey(ScheduleId scheduleId, TaskId taskId)
    {
        return ZKPaths.makePath(TASK_LOCKS_PATH, scheduleId.getId() + SEPARATOR + taskId.getId());
    }

    public static String getCompletedTaskKey(ScheduleId scheduleId, TaskId taskId)
    {
        return ZKPaths.makePath(COMPLETED_TASKS_PATH, scheduleId.getId() + SEPARATOR + taskId.getId());
    }
}
