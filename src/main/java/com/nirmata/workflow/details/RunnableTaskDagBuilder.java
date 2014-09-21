package com.nirmata.workflow.details;

import com.google.common.collect.Lists;
import com.nirmata.workflow.details.internalmodels.RunnableTaskDagEntryModel;
import com.nirmata.workflow.details.internalmodels.RunnableTaskDagModel;
import com.nirmata.workflow.models.TaskDagModel;
import com.nirmata.workflow.models.TaskId;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.traverse.TopologicalOrderIterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class RunnableTaskDagBuilder
{
    private final TaskDagModel taskDag;

    private static class TaskIdEdge extends DefaultEdge
    {
        private final TaskId parentTaskId;

        public TaskIdEdge(TaskId parentTaskId)
        {
            this.parentTaskId = parentTaskId;
        }

        public TaskId getParentTaskId()
        {
            return parentTaskId;
        }

        @Override
        public String toString()
        {
            return parentTaskId.getId();
        }
    }

    public RunnableTaskDagBuilder(TaskDagModel taskDag)
    {
        this.taskDag = taskDag;
    }

    public RunnableTaskDagModel build()
    {
        DefaultDirectedGraph<TaskId, TaskIdEdge> graph = new DefaultDirectedGraph<>(TaskIdEdge.class);
        worker(graph, taskDag, null);

        List<RunnableTaskDagEntryModel> entries = Lists.newArrayList();
        TopologicalOrderIterator<TaskId, TaskIdEdge> orderIterator = new TopologicalOrderIterator(graph);
        while ( orderIterator.hasNext() )
        {
            TaskId taskId = orderIterator.next();
            Set<TaskIdEdge> taskIdEdges = graph.edgesOf(taskId);
            Set<TaskId> processed = taskIdEdges
                .stream()
                .filter(edge -> !edge.getParentTaskId().equals(taskId) && !edge.getParentTaskId().getId().equals(""))
                .map(TaskIdEdge::getParentTaskId)
                .collect(Collectors.toSet());
            entries.add(new RunnableTaskDagEntryModel(taskId, processed));
        }
        return new RunnableTaskDagModel(entries);
    }

    private static void worker(DefaultDirectedGraph<TaskId, TaskIdEdge> graph, TaskDagModel taskDag, TaskId parentId)
    {
        graph.addVertex(taskDag.getTaskId());
        if ( parentId != null )
        {
            graph.addEdge(parentId, taskDag.getTaskId(), new TaskIdEdge(parentId));
        }
        taskDag.getChildren().forEach(child -> worker(graph, child, taskDag.getTaskId()));
    }
}
