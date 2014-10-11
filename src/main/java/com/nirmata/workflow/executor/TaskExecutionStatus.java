package com.nirmata.workflow.executor;

/**
 * Task execution status
 */
public enum TaskExecutionStatus
{
    /**
     * The task executed successfully
     */
    SUCCESS()
    {
        @Override
        public boolean isCancelingStatus()
        {
            return false;
        }
    },

    /**
     * The task failed, but the remaining tasks should still execute
     */
    FAILED_CONTINUE()
    {
        @Override
        public boolean isCancelingStatus()
        {
            return false;
        }
    },

    /**
     * The task failed and the remaining tasks in the run should be canceled
     */
    FAILED_STOP()
    {
        @Override
        public boolean isCancelingStatus()
        {
            return true;
        }
    }
    ;

    public abstract boolean isCancelingStatus();
}
