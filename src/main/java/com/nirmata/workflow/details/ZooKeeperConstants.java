package com.nirmata.workflow.details;

import com.nirmata.workflow.models.RunId;
import com.nirmata.workflow.models.TaskId;
import org.apache.curator.utils.ZKPaths;

public class ZooKeeperConstants
{
    private static final String SCHEDULER_LEADER_PATH = "/scheduler-leader";
    private static final String RUNS_PATH = "/runs";
    private static final String RUNS_WORK_PATH = "/runs-work";
    private static final String COMPLETED_RUNS_PARENT_PATH = "/completed-runs";
    private static final String COMPLETED_TASKS_PATH = "/tasks-completed";
    private static final String STARTED_TASKS_PATH = "/tasks-started";
    private static final String IDEMPOTENT_TASKS_QUEUE_PATH = "/tasks-queue";
    private static final String NON_IDEMPOTENT_TASKS_QUEUE_PATH = "/tasks-queue-non";
    private static final String IDEMPOTENT_TASKS_QUEUE_LOCK_PATH = "/tasks-queue-locks";

    public static final int MAX_PAYLOAD = 0xfffff;  // see "jute.maxbuffer" at http://zookeeper.apache.org/doc/r3.3.1/zookeeperAdmin.html

    private ZooKeeperConstants()
    {
    }

    public static void main(String[] args)
    {
        TaskId taskId = new TaskId();
        RunId runId = new RunId();
        String runPath = getRunPath(runId);
        String completedTaskPath = getCompletedTaskPath(runId, taskId);

        System.out.println("taskId = " + taskId.getId());
        System.out.println("runId = " + runId.getId());
        System.out.println();

        System.out.println("getRunIdFromRunPath:\t\t\t\t" + getRunIdFromRunPath(runPath));
        System.out.println("getRunIdFromCompletedTaskPath:\t\t" + getRunIdFromCompletedTaskPath(completedTaskPath));
        System.out.println();

        System.out.println("getSchedulerLeaderPath:\t\t\t\t" + getSchedulerLeaderPath());
        System.out.println("getRunsParentPath:\t\t\t\t\t" + getRunsParentPath());
        System.out.println("getRunPath:\t\t\t\t\t\t\t" + getRunPath(runId));
        System.out.println("getIdempotentTasksQueuePath:\t\t" + getIdempotentTasksQueuePath());
        System.out.println("getIdempotentTasksQueueLockPath:\t" + getIdempotentTasksQueueLockPath());
        System.out.println("getNonIdempotentTasksQueuePath:\t\t" + getNonIdempotentTasksQueuePath());
        System.out.println("getCompletedRunParentPath:\t\t\t" + getCompletedRunParentPath());
        System.out.println("getCompletedRunPath:\t\t\t\t" + getCompletedRunPath(runId));
        System.out.println("getCompletedTasksParentPath:\t\t" + getCompletedTasksParentPath(runId));
        System.out.println("getCompletedTaskPath:\t\t\t\t" + completedTaskPath);
        System.out.println("getStartedTaskPath:\t\t\t\t\t" + getStartedTaskPath(runId, taskId));
    }

    public static String getSchedulerLeaderPath()
    {
        return SCHEDULER_LEADER_PATH;
    }

    public static String getRunsParentPath()
    {
        return RUNS_PATH;
    }

    public static String getRunPath(RunId runId)
    {
        return ZKPaths.makePath(RUNS_PATH, runId.getId());
    }

    public static String getRunIdFromRunPath(String path)
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

    public static String getCompletedRunParentPath()
    {
        return COMPLETED_RUNS_PARENT_PATH;
    }

    public static String getCompletedRunPath(RunId runId)
    {
        return ZKPaths.makePath(COMPLETED_RUNS_PARENT_PATH, runId.getId());
    }

    private static String getRunsWorkParentPath(RunId runId)
    {
        return ZKPaths.makePath(RUNS_WORK_PATH, runId.getId());
    }

    public static String getCompletedTasksParentPath(RunId runId)
    {
        return ZKPaths.makePath(getRunsWorkParentPath(runId), COMPLETED_TASKS_PATH);
    }

    public static String getCompletedTaskPath(RunId runId, TaskId taskId)
    {
        return ZKPaths.makePath(getCompletedTasksParentPath(runId), taskId.getId());
    }

    public static String getTaskIdFromCompletedTaskPath(String path)
    {
        ZKPaths.PathAndNode pathAndNode = ZKPaths.getPathAndNode(path);
        return pathAndNode.getNode();
    }

    public static String getRunIdFromCompletedTaskPath(String path)
    {
        ZKPaths.PathAndNode pathAndNode = ZKPaths.getPathAndNode(path);
        ZKPaths.PathAndNode parentPathAndNode = ZKPaths.getPathAndNode(pathAndNode.getPath());
        ZKPaths.PathAndNode grandParentPathAndNode = ZKPaths.getPathAndNode(parentPathAndNode.getPath());
        return grandParentPathAndNode.getNode();
    }

    public static String getStartedTaskPath(RunId runId, TaskId taskId)
    {
        String parentPath = ZKPaths.makePath(getRunsWorkParentPath(runId), STARTED_TASKS_PATH);
        return ZKPaths.makePath(parentPath, taskId.getId());
    }
}
