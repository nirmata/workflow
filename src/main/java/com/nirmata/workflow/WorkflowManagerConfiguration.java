package com.nirmata.workflow;

public interface WorkflowManagerConfiguration
{
    public int getStorageRefreshMs();

    public boolean canBeScheduler();

    public int getMaxTaskRunners();

    public int getSchedulerSleepMs();

    public int getTaskRunnerSleepMs();
}
