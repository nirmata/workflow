package com.nirmata.workflow.models;

public enum TaskMode
{
    STANDARD(0),
    DELAY(1),
    PRIORITY(2)
    ;

    public static TaskMode fromCode(int code)
    {
        for ( TaskMode mode : values() )
        {
            if ( mode.code == code )
            {
                return mode;
            }
        }
        return null;
    }

    public int getCode()
    {
        return code;
    }

    private final int code;

    TaskMode(int code)
    {
        this.code = code;
    }
}
