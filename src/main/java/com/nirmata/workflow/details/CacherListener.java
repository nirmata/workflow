package com.nirmata.workflow.details;

import com.nirmata.workflow.details.internalmodels.DenormalizedWorkflowModel;

@FunctionalInterface
interface CacherListener
{
    public void updateAndQueueTasks(Cacher cacher, DenormalizedWorkflowModel workflow);
}
