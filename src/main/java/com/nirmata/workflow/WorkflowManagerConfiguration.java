package com.nirmata.workflow;

import com.nirmata.workflow.spi.StorageBridge;

/**
 * Configuration values for the workflow
 */
public interface WorkflowManagerConfiguration
{
    /**
     * How often to refresh the data from {@link StorageBridge}
     *
     * @return refresh time in milliseconds
     */
    public int getStorageRefreshMs();

    /**
     * How long scheduler should sleep between schedule checks
     *
     * @return scheduler sleep in milliseconds
     */
    public int getSchedulerSleepMs();

    /**
     * Return the max number of idempotent tasks to concurrently allow
     *
     * @return max
     */
    public int getIdempotentTaskQty();

    /**
     * Return the max number of non-idempotent tasks to concurrently allow
     *
     * @return max
     */
    public int getNonIdempotentTaskQty();
}
