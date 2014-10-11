package com.nirmata.workflow;

import com.nirmata.workflow.executor.TaskExecutor;
import com.nirmata.workflow.models.RunId;
import com.nirmata.workflow.models.Task;
import com.nirmata.workflow.models.TaskId;
import org.apache.curator.framework.CuratorFramework;
import java.io.Closeable;
import java.util.Map;
import java.util.Optional;

/**
 * Main API - create via {@link WorkflowManagerBuilder}
 */
public interface WorkflowManager extends Closeable
{
    /**
     * <p>
     *     The manager must be started before use. Call {@link #close()} when done
     *     with the manager. Every instance that starts a manager using
     *     the same {@link WorkflowManagerBuilder#withCurator(CuratorFramework, String, String)} <code>namespace</code>
     *     and <code>version</code> will participate in the workflow.
     * </p>
     *
     * <p>
     *     One instance will be nominated as the scheduler and will be
     *     responsible for starting tasks and advancing the workflow.
     * </p>
     *
     * <p>
     *     Task executors will be started based on the values in the {@link WorkflowManagerBuilder}.
     *     Each WorkflowManager can declare a different combination of task executors
     *     as needed.
     * </p>
     */
    public void start();

    /**
     * Submit a task for execution. The task will start nearly immediately. There's no
     * guarantee which instances will execute the various tasks. This method can be called
     * by any instance. i.e. it's not necessary that this be called from the currently
     * nominated scheduler instance.
     *
     * @param task task to execute
     * @return the Run ID for the task
     */
    public RunId submitTask(Task task);

    /**
     * Same as {@link #submitTask(Task)} except that, when completed, the parent run will
     * be notified. This method is meant to be used inside of {@link TaskExecutor} for a task
     * that needs to initiate a sub-run and have the parent run wait for the sub-run to complete.
     *
     * @param parentRunId run id of the parent run
     * @param task task to execute
     * @return sub-Run ID
     */
    public RunId submitSubTask(RunId parentRunId, Task task);

    /**
     * Attempt to cancel the given run. NOTE: the cancellation is scheduled and does not
     * occur immediately. Currently executing tasks will continue. Only tasks that have not yet
     * executed can be canceled.
     *
     * @param runId the run to cancel
     * @return true if the run was found and the cancellation was scheduled
     */
    public boolean cancelRun(RunId runId);

    /**
     * Return the generated result data for a given task of a given run
     *
     * @param runId the run
     * @param taskId the task
     * @return if found, a loaded optional with the result data. Otherwise, an empty optional.
     */
    public Optional<Map<String, String>> getTaskData(RunId runId, TaskId taskId);
}
