package com.nirmata.workflow.details;

import com.nirmata.workflow.details.internalmodels.DenormalizedWorkflowModel;

interface CacherListener
{
    public void updateAndQueueTasks(Cacher cacher, DenormalizedWorkflowModel workflow);
}
