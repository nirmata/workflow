package com.nirmata.workflow.admin;

import com.nirmata.workflow.models.RunId;
import java.util.List;

/**
 * Admin operations
 */
public interface WorkflowAdmin
{
    /**
     * Return info about all runs completed or currently executing
     * in the workflow manager
     *
     * @return run infos
     */
    public List<RunInfo> getRunInfo();

    /**
     * Return info about the given run
     *
     * @param runId run
     * @return info
     */
    public RunInfo getRunInfo(RunId runId);

    /**
     * Return info about all the tasks completed, started or waiting for
     * the given run
     *
     * @param runId run
     * @return task infos
     */
    public List<TaskInfo> getTaskInfo(RunId runId);

    /**
     * Delete all saved data for the given run.
     *
     * @param runId the run
     * @return true if the run was found
     */
    public boolean clean(RunId runId);
}
