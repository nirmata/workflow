package com.nirmata.workflow.admin;

import com.nirmata.workflow.WorkflowManager;
import com.nirmata.workflow.details.InternalJsonSerializer;
import com.nirmata.workflow.details.Scheduler;
import com.nirmata.workflow.details.WorkflowStatus;
import com.nirmata.workflow.details.ZooKeeperConstants;
import com.nirmata.workflow.details.internalmodels.DenormalizedWorkflowModel;
import com.nirmata.workflow.models.RunId;
import com.nirmata.workflow.spi.JsonSerializer;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Stopper
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final WorkflowManager workflowManager;

    public Stopper(WorkflowManager workflowManager)
    {
        this.workflowManager = workflowManager;
    }

    public boolean stop(RunId runId)
    {
        String runPath = ZooKeeperConstants.getRunPath(runId);
        try
        {
            byte[] bytes = workflowManager.getCurator().getData().forPath(runPath);
            DenormalizedWorkflowModel denormalizedWorkflow = InternalJsonSerializer.getDenormalizedWorkflow(JsonSerializer.fromBytes(bytes));
            return Scheduler.completeWorkflow(null, log, workflowManager, denormalizedWorkflow, WorkflowStatus.FORCE_FAILED);
        }
        catch ( KeeperException.NoNodeException ignore )
        {
            // ignore
        }
        catch ( Exception e )
        {
            String message = "Could not get data for path: " + runPath;
            log.error(message, e);
            throw new RuntimeException(message, e);
        }
        return false;
    }
}
