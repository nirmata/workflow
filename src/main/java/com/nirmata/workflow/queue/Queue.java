package com.nirmata.workflow.queue;

import com.nirmata.workflow.details.internalmodels.ExecutableTaskModel;
import java.io.Closeable;

public interface Queue extends Closeable
{
    public void start();

    @Override
    public void close();

    public void put(ExecutableTaskModel executableTask);
}
