package com.nirmata.workflow.details;

import java.util.concurrent.TimeUnit;

class TimedGate
{
    synchronized void forceOpen()
    {
        notifyAll();
    }

    synchronized void closeAndWaitForOpen(long maxTime, TimeUnit timeUnit) throws InterruptedException
    {
        wait(timeUnit.toMillis(maxTime));
    }
}
