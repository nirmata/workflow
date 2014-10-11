package com.nirmata.workflow.events;

@FunctionalInterface
public interface WorkflowListener
{
    /**
     * Receive a workflow event
     *
     * @param event the event
     */
    public void receiveEvent(WorkflowEvent event);
}
