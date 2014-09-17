package com.nirmata.workflow;

public class WorkflowManagerConfigurationImpl implements WorkflowManagerConfiguration
{
    private int storageRefreshMs;
    private int schedulerSleepMs;
    private int idempotentTaskQty;
    private int nonIdempotentTaskQty;

    public WorkflowManagerConfigurationImpl()
    {
    }

    public WorkflowManagerConfigurationImpl(int storageRefreshMs, int schedulerSleepMs, int idempotentTaskQty, int nonIdempotentTaskQty)
    {
        this.storageRefreshMs = storageRefreshMs;
        this.schedulerSleepMs = schedulerSleepMs;
        this.idempotentTaskQty = idempotentTaskQty;
        this.nonIdempotentTaskQty = nonIdempotentTaskQty;
    }

    @Override
    public int getStorageRefreshMs()
    {
        return storageRefreshMs;
    }

    @Override
    public int getSchedulerSleepMs()
    {
        return schedulerSleepMs;
    }

    @Override
    public int getIdempotentTaskQty()
    {
        return idempotentTaskQty;
    }

    @Override
    public int getNonIdempotentTaskQty()
    {
        return nonIdempotentTaskQty;
    }

    public void setStorageRefreshMs(int storageRefreshMs)
    {
        this.storageRefreshMs = storageRefreshMs;
    }

    public void setSchedulerSleepMs(int schedulerSleepMs)
    {
        this.schedulerSleepMs = schedulerSleepMs;
    }

    public void setIdempotentTaskQty(int idempotentTaskQty)
    {
        this.idempotentTaskQty = idempotentTaskQty;
    }

    public void setNonIdempotentTaskQty(int nonIdempotentTaskQty)
    {
        this.nonIdempotentTaskQty = nonIdempotentTaskQty;
    }
}
