package com.nirmata.workflow;

public interface WorkflowManagerConfiguration
{
    public int getStorageRefreshMs();

    public int getSchedulerSleepMs();

    public int getIdempotentTaskQty();

    public int getNonIdempotentTaskQty();
}
