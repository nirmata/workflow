package com.nirmata.workflow.details;

import com.nirmata.workflow.models.ScheduleId;
import com.nirmata.workflow.models.TaskId;
import org.apache.curator.utils.ZKPaths;

public class ZooKeeperConstants
{
    private static final String SCHEDULER_LEADER_PATH = "/scheduler-leader";
    private static final String SCHEDULES_PATH = "/schedules";
    private static final String COMPLETED_SCHEDULES_PATH = "/completed-schedules";
    private static final String COMPLETED_TASKS_PATH = "/tasks-completed";
    private static final String IDEMPOTENT_TASKS_QUEUE_PATH = "/tasks-queue";
    private static final String NON_IDEMPOTENT_TASKS_QUEUE_PATH = "/tasks-queue-non";
    private static final String IDEMPOTENT_TASKS_QUEUE_LOCK_PATH = "/tasks-queue-locks";

    public static final int MAX_PAYLOAD = 0xfffff;  // see "jute.maxbuffer" at http://zookeeper.apache.org/doc/r3.3.1/zookeeperAdmin.html

    private ZooKeeperConstants()
    {
    }

    public static String getSchedulerLeaderPath()
    {
        return SCHEDULER_LEADER_PATH;
    }

    public static String getScheduleParentPath()
    {
        return SCHEDULES_PATH;
    }

    public static String getSchedulePath(ScheduleId scheduleId)
    {
        return ZKPaths.makePath(SCHEDULES_PATH, scheduleId.getId());
    }

    public static String getScheduleIdFromSchedulePath(String key)
    {
        return ZKPaths.getPathAndNode(key).getNode();
    }

    public static String getIdempotentTasksQueuePath()
    {
        return IDEMPOTENT_TASKS_QUEUE_PATH;
    }

    public static String getIdempotentTasksQueueLockPath()
    {
        return IDEMPOTENT_TASKS_QUEUE_LOCK_PATH;
    }

    public static String getNonIdempotentTasksQueuePath()
    {
        return NON_IDEMPOTENT_TASKS_QUEUE_PATH;
    }

    public static String getCompletedSchedulePath(ScheduleId scheduleId)
    {
        return ZKPaths.makePath(COMPLETED_SCHEDULES_PATH, scheduleId.getId());
    }

    public static String getCompletedTasksParentPath(ScheduleId scheduleId)
    {
        return ZKPaths.makePath(getSchedulePath(scheduleId), COMPLETED_TASKS_PATH);
    }

    public static String getScheduleIdFromCompletedTaskPath(String path)
    {
        ZKPaths.PathAndNode pathAndNode = ZKPaths.getPathAndNode(path);
        ZKPaths.PathAndNode parentPathAndNode = ZKPaths.getPathAndNode(pathAndNode.getPath());
        return ZKPaths.makePath(parentPathAndNode.getPath(), parentPathAndNode.getNode());
    }

    public static String getCompletedTaskPath(ScheduleId scheduleId, TaskId taskId)
    {
        return ZKPaths.makePath(getCompletedTasksParentPath(scheduleId), taskId.getId());
    }
}
