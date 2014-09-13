package com.nirmata.workflow.details;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import com.nirmata.workflow.models.TaskId;
import com.nirmata.workflow.models.TaskModel;
import com.nirmata.workflow.models.WorkflowId;
import com.nirmata.workflow.models.WorkflowModel;
import com.nirmata.workflow.spi.JsonSerializer;
import java.util.List;

public class InternalJsonSerializer
{
    public static void addDenormalizedWorkflow(ObjectNode node, StateCache cache, WorkflowId workflowId)
    {
        WorkflowModel workflow = cache.getWorkflows().get(workflowId);
        if ( workflow == null )
        {
            // TODO
        }
        List<TaskModel> tasks = Lists.newArrayList();
        for ( List<TaskId> thisSet : workflow.getTasks() )
        {
            ArrayNode tab = JsonSerializer.newArrayNode();
            for ( TaskId taskId : thisSet )
            {
                TaskModel task = cache.getTasks().get(taskId);
                if ( task == null )
                {
                    // TODO
                }
                tasks.add(task);
            }
        }

        node.put("workflowId", workflowId.getId());
        node.put("name", workflow.getName());
        JsonSerializer.addTaskSet(node, workflow.getTasks());
        JsonSerializer.addTasks(node, tasks);
        node.put("startDate", JsonSerializer.toString(Clock.nowUtc()));

        // TODO
    }
}
