package com.nirmata.workflow.executor;

public enum TaskExecutionStatus
{
    SUCCESS()
    {
        @Override
        public boolean isCancelingStatus()
        {
            return false;
        }
    },

    FAILED_CONTINUE()
    {
        @Override
        public boolean isCancelingStatus()
        {
            return false;
        }
    },

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
