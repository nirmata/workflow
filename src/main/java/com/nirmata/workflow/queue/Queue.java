package com.nirmata.workflow.queue;

import com.nirmata.workflow.models.ExecutableTask;
import java.io.Closeable;

public interface Queue extends Closeable
{
    public void start();

    @Override
    public void close();

    public void put(ExecutableTask executableTask);
}
