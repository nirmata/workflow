package com.nirmata.workflow.details;

import com.nirmata.workflow.models.ScheduleId;
import com.nirmata.workflow.models.TaskId;
import org.apache.curator.utils.ZKPaths;

public class ZooKeeperConstants
{
    private static final String SCHEDULER_LEADER_PATH = "/scheduler-leader";
    private static final String SCHEDULES_PATH = "/schedules";
    private static final String SCHEDULES_WORK_PATH = "/schedules-work";
    private static final String COMPLETED_SCHEDULES_PARENT_PATH = "/completed-schedules";
    private static final String COMPLETED_TASKS_PATH = "/tasks-completed";
    private static final String STARTED_TASKS_PATH = "/tasks-started";
    private static final String IDEMPOTENT_TASKS_QUEUE_PATH = "/tasks-queue";
    private static final String NON_IDEMPOTENT_TASKS_QUEUE_PATH = "/tasks-queue-non";
    private static final String IDEMPOTENT_TASKS_QUEUE_LOCK_PATH = "/tasks-queue-locks";

    private static final String COMPLETED_SCHEDULE_BASE_NAME = "instance-";

    public static final int MAX_PAYLOAD = 0xfffff;  // see "jute.maxbuffer" at http://zookeeper.apache.org/doc/r3.3.1/zookeeperAdmin.html

    private ZooKeeperConstants()
    {
    }

    public static void main(String[] args)
    {
        ScheduleId scheduleId = new ScheduleId();
        TaskId taskId = new TaskId();
        String schedulePath = getSchedulePath(scheduleId);
        String completedTaskPath = getCompletedTaskPath(scheduleId, taskId);

        System.out.println("scheduleId = " + scheduleId.getId());
        System.out.println("taskId = " + taskId.getId());
        System.out.println();

        System.out.println("getScheduleIdFromSchedulePath:\t\t" + getScheduleIdFromSchedulePath(schedulePath));
        System.out.println("getScheduleIdFromCompletedTaskPath:\t" + getScheduleIdFromCompletedTaskPath(completedTaskPath));
        System.out.println();

        System.out.println("getSchedulerLeaderPath:\t\t\t\t" + getSchedulerLeaderPath());
        System.out.println("getScheduleParentPath:\t\t\t\t" + getScheduleParentPath());
        System.out.println("getSchedulePath:\t\t\t\t\t" + schedulePath);
        System.out.println("getIdempotentTasksQueuePath:\t\t" + getIdempotentTasksQueuePath());
        System.out.println("getIdempotentTasksQueueLockPath:\t" + getIdempotentTasksQueueLockPath());
        System.out.println("getNonIdempotentTasksQueuePath:\t\t" + getNonIdempotentTasksQueuePath());
        System.out.println("getCompletedScheduleParentPath:\t\t" + getCompletedScheduleParentPath(scheduleId));
        System.out.println("getCompletedScheduleBasePath:\t\t" + getCompletedScheduleBasePath(scheduleId));
        System.out.println("getCompletedTasksParentPath:\t\t" + getCompletedTasksParentPath(scheduleId));
        System.out.println("getCompletedTaskPath:\t\t\t\t" + completedTaskPath);
        System.out.println("getStartedTaskPath:\t\t\t\t\t" + getStartedTaskPath(scheduleId, taskId));
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

    public static String getScheduleIdFromSchedulePath(String path)
    {
        return ZKPaths.getPathAndNode(path).getNode();
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

    public static String getCompletedScheduleParentPath(ScheduleId scheduleId)
    {
        return ZKPaths.makePath(COMPLETED_SCHEDULES_PARENT_PATH, scheduleId.getId());
    }

    public static String getCompletedScheduleBasePath(ScheduleId scheduleId)
    {
        return ZKPaths.makePath(getCompletedScheduleParentPath(scheduleId), COMPLETED_SCHEDULE_BASE_NAME);
    }

    private static String getSchedulesWorkParentPath(ScheduleId scheduleId)
    {
        return ZKPaths.makePath(SCHEDULES_WORK_PATH, scheduleId.getId());
    }

    public static String getCompletedTasksParentPath(ScheduleId scheduleId)
    {
        return ZKPaths.makePath(getSchedulesWorkParentPath(scheduleId), COMPLETED_TASKS_PATH);
    }

    public static String getCompletedTaskPath(ScheduleId scheduleId, TaskId taskId)
    {
        return ZKPaths.makePath(getCompletedTasksParentPath(scheduleId), taskId.getId());
    }

    public static String getScheduleIdFromCompletedTaskPath(String path)
    {
        ZKPaths.PathAndNode pathAndNode = ZKPaths.getPathAndNode(path);
        ZKPaths.PathAndNode parentPathAndNode = ZKPaths.getPathAndNode(pathAndNode.getPath());
        ZKPaths.PathAndNode grandParentPathAndNode = ZKPaths.getPathAndNode(parentPathAndNode.getPath());
        return grandParentPathAndNode.getNode();
    }

    public static String getStartedTaskPath(ScheduleId scheduleId, TaskId taskId)
    {
        String parentPath = ZKPaths.makePath(getSchedulesWorkParentPath(scheduleId), STARTED_TASKS_PATH);
        return ZKPaths.makePath(parentPath, taskId.getId());
    }
}
