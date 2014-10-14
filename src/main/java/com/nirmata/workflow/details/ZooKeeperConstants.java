/**
 * Copyright 2014 Nirmata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nirmata.workflow.details;

import com.google.common.base.Splitter;
import com.nirmata.workflow.models.RunId;
import com.nirmata.workflow.models.TaskId;
import com.nirmata.workflow.models.TaskType;
import org.apache.curator.utils.ZKPaths;

public class ZooKeeperConstants
{
    private static final String SCHEDULER_LEADER_PATH = "/scheduler-leader";
    private static final String RUN_PATH = "/runs";
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
        String startedTaskPath = getStartedTaskPath(runId, taskId);
        TaskType taskType = new TaskType("a", "b", true);

        System.out.println("taskId = " + taskId.getId());
        System.out.println("runId = " + runId.getId());
        System.out.println();

        System.out.println("getRunIdFromRunPath:\t\t\t\t" + getRunIdFromRunPath(runPath));
        System.out.println("getRunIdFromCompletedTasksPath:\t\t" + getRunIdFromCompletedTasksPath(completedTaskPath));
        System.out.println("getTaskIdFromCompletedTasksPath:\t" + getTaskIdFromCompletedTasksPath(completedTaskPath));
        System.out.println("getTaskIdFromStartedTasksPath:\t\t" + getTaskIdFromStartedTasksPath(startedTaskPath));
        System.out.println();

        System.out.println("getSchedulerLeaderPath:\t\t\t\t" + getSchedulerLeaderPath());
        System.out.println("getRunParentPath:\t\t\t\t\t" + getRunParentPath());
        System.out.println("getRunPath:\t\t\t\t\t\t\t" + getRunPath(runId));
        System.out.println("getQueueBasePath:\t\t\t\t\t" + getQueueBasePath(taskType));
        System.out.println("getQueuePath:\t\t\t\t\t\t" + getQueuePath(taskType));
        System.out.println("getQueueLockPath:\t\t\t\t\t" + getQueueLockPath(taskType));
        System.out.println("getCompletedTasksParentPath:\t\t" + getCompletedTaskParentPath());
        System.out.println("getCompletedTaskPath:\t\t\t\t" + completedTaskPath);
        System.out.println("getStartedTasksParentPath:\t\t\t" + getStartedTasksParentPath());
        System.out.println("getStartedTaskPath:\t\t\t\t\t" + startedTaskPath);
    }

    public static String getSchedulerLeaderPath()
    {
        return SCHEDULER_LEADER_PATH;
    }

    public static String getRunParentPath()
    {
        return RUN_PATH;
    }

    public static String getRunPath(RunId runId)
    {
        return ZKPaths.makePath(RUN_PATH, runId.getId());
    }

    public static String getRunIdFromRunPath(String path)
    {
        return ZKPaths.getPathAndNode(path).getNode();
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

    public static String getCompletedTaskParentPath()
    {
        return COMPLETED_TASKS_PATH;
    }

    public static String getRunIdFromCompletedTasksPath(String path)
    {
        String n = ZKPaths.getNodeFromPath(path);
        return Splitter.on(SEPARATOR).splitToList(n).get(0);
    }

    public static String getTaskIdFromStartedTasksPath(String path)
    {
        return getTaskIdFromCompletedTasksPath(path);
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
