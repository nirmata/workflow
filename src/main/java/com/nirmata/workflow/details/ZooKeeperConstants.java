package com.nirmata.workflow.details;

import com.google.common.base.Splitter;
import com.nirmata.workflow.models.RunId;
import com.nirmata.workflow.models.TaskId;
import com.nirmata.workflow.models.TaskType;
import org.apache.curator.utils.ZKPaths;

public class ZooKeeperConstants
{
    private static final String SCHEDULER_LEADER_PATH = "/scheduler-leader";
    private static final String RUNS_PATH = "/runs";
    private static final String COMPLETED_RUNS_PARENT_PATH = "/completed-runs";
    private static final String COMPLETED_TASKS_PATH = "/tasks-completed";
    private static final String STARTED_TASKS_PATH = "/tasks-started";
    private static final String QUEUE_PATH_BASE = "/tasks-queue";

    private static final String SEPARATOR = "|";

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
        System.out.println("getRunIdFromCompletedTasksPath:\t\t" + getRunIdFromCompletedTasksPath(completedTaskPath));
        System.out.println();

        System.out.println("getSchedulerLeaderPath:\t\t\t\t" + getSchedulerLeaderPath());
        System.out.println("getRunsParentPath:\t\t\t\t\t" + getRunParentPath());
        System.out.println("getRunPath:\t\t\t\t\t\t\t" + getRunPath(runId));
        System.out.println("getQueuePath:\t\t\t\t\t\t" + getQueuePath(new TaskType("a", "b", true)));
        System.out.println("getQueueLockPath:\t\t\t\t\t" + getQueueLockPath(new TaskType("a", "b", true)));
        System.out.println("getCompletedRunParentPath:\t\t\t" + getCompletedRunParentPath());
        System.out.println("getCompletedRunPath:\t\t\t\t" + getCompletedRunPath(runId));
        System.out.println("getCompletedTasksParentPath:\t\t" + getCompletedTaskParentPath());
        System.out.println("getCompletedTaskPath:\t\t\t\t" + completedTaskPath);
        System.out.println("getStartedTaskPath:\t\t\t\t\t" + getStartedTaskPath(runId, taskId));
    }

    public static String getSchedulerLeaderPath()
    {
        return SCHEDULER_LEADER_PATH;
    }

    public static String getRunParentPath()
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

    public static String getRunIdFromCompletedRunPath(String path)
    {
        return getRunIdFromRunPath(path);
    }

    public static String getQueuePath(TaskType taskType)
    {
        String path = getQueueBasePath(taskType);
        return ZKPaths.makePath(path, "queue");
    }

    public static String getQueueLockPath(TaskType taskType)
    {
        String path = getQueueBasePath(taskType);
        return ZKPaths.makePath(path, "queue-lock");
    }

    private static String getQueueBasePath(TaskType taskType)
    {
        String path = ZKPaths.makePath(QUEUE_PATH_BASE, taskType.getType());
        path = ZKPaths.makePath(path, taskType.isIdempotent() ? "i" : "ni");
        return ZKPaths.makePath(path, taskType.getVersion());
    }

    public static String getCompletedRunParentPath()
    {
        return COMPLETED_RUNS_PARENT_PATH;
    }

    public static String getCompletedRunPath(RunId runId)
    {
        return ZKPaths.makePath(COMPLETED_RUNS_PARENT_PATH, runId.getId());
    }

    public static String getCompletedTaskParentPath()
    {
        return COMPLETED_TASKS_PATH;
    }

    public static String getRunIdFromCompletedTasksPath(String path)
    {
        String n = ZKPaths.getNodeFromPath(path);
        return Splitter.on(SEPARATOR).splitToList(n).get(0);
    }

    public static String getTaskIdFromCompletedTasksPath(String path)
    {
        String n = ZKPaths.getNodeFromPath(path);
        return Splitter.on(SEPARATOR).splitToList(n).get(1);
    }

    public static String getCompletedTaskPath(RunId runId, TaskId taskId)
    {
        return ZKPaths.makePath(getCompletedTaskParentPath(), makeRunTask(runId, taskId));
    }

    public static String getStartedTasksParentPath()
    {
        return STARTED_TASKS_PATH;
    }

    public static String getStartedTaskPath(RunId runId, TaskId taskId)
    {
        String parentPath = getStartedTasksParentPath();
        return ZKPaths.makePath(parentPath, makeRunTask(runId, taskId));
    }

    public static String getRunIdFromStartedTasksPath(String path)
    {
        return getRunIdFromCompletedTasksPath(path);
    }

    private static String makeRunTask(RunId runId, TaskId taskId)
    {
        return runId.getId() + SEPARATOR + taskId.getId();
    }
}
