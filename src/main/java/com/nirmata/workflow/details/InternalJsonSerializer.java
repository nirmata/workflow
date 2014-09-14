package com.nirmata.workflow.details;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import com.nirmata.workflow.details.internalmodels.CompletedTaskModel;
import com.nirmata.workflow.details.internalmodels.DenormalizedWorkflowModel;
import com.nirmata.workflow.details.internalmodels.ExecutableTaskModel;
import com.nirmata.workflow.models.ScheduleId;
import com.nirmata.workflow.models.TaskId;
import com.nirmata.workflow.models.TaskModel;
import com.nirmata.workflow.models.WorkflowId;
import com.nirmata.workflow.models.WorkflowModel;
import com.nirmata.workflow.spi.JsonSerializer;
import java.util.Date;
import java.util.List;

public class InternalJsonSerializer
{
    public static ObjectNode addCompletedTask(ObjectNode node, CompletedTaskModel completedTask)
    {
        node.put("isComplete", completedTask.isComplete());
        node.putPOJO("resultData", completedTask.getResultData());
        return node;
    }

    public static void addExecutableTask(ObjectNode node, ExecutableTaskModel executableTask)
    {
        node.put("scheduleId", executableTask.getScheduleId().getId());
        JsonSerializer.addTask(node, executableTask.getTask());
    }

    public static ExecutableTaskModel getExecutableTask(JsonNode node)
    {
        return new ExecutableTaskModel
        (
            new ScheduleId(node.get("scheduleId").asText()),
            JsonSerializer.getTask(node)
        );
    }

    public static CompletedTaskModel getCompletedTask(JsonNode node)
    {
        return new CompletedTaskModel
        (
            node.get("isComplete").asBoolean(),
            JsonSerializer.getMap(node.get("resultData"))
        );
    }

    public static void addDenormalizedWorkflow(ObjectNode node, StateCache cache, WorkflowId workflowId, Date nowUtc)
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
        node.put("startDate", JsonSerializer.toString(nowUtc));

        // TODO
    }

    public static DenormalizedWorkflowModel getDenormalizedWorkflow(JsonNode node)
    {
        return new DenormalizedWorkflowModel
        (
            new WorkflowId(node.get("workflowId").asText()),
            JsonSerializer.getTasks(node),
            node.get("name").asText(),
            JsonSerializer.getTaskSet(node),
            JsonSerializer.dateFromString(node.get("startDate").asText())
        );
    }

    private InternalJsonSerializer()
    {
    }
}
