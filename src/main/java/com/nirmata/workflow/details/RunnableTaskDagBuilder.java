package com.nirmata.workflow.details;

import com.google.common.collect.Lists;
import com.nirmata.workflow.details.internalmodels.RunnableTaskDagEntryModel;
import com.nirmata.workflow.details.internalmodels.RunnableTaskDagModel;
import com.nirmata.workflow.models.TaskDagModel;
import com.nirmata.workflow.models.TaskId;
import org.jgrapht.alg.CycleDetector;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.traverse.TopologicalOrderIterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class RunnableTaskDagBuilder
{
    private final TaskDagModel taskDag;

    public RunnableTaskDagBuilder(TaskDagModel taskDag)
    {
        this.taskDag = taskDag;
    }

    public RunnableTaskDagModel build()
    {
        DefaultDirectedGraph<TaskId, DefaultEdge> graph = new DefaultDirectedGraph<>(DefaultEdge.class);
        worker(graph, taskDag, null);

        CycleDetector<TaskId, DefaultEdge> cycleDetector = new CycleDetector<>(graph);
        if ( cycleDetector.detectCycles() )
        {
            throw new RuntimeException("The Task DAG contains cycles: " + taskDag);
        }

        List<RunnableTaskDagEntryModel> entries = Lists.newArrayList();
        TopologicalOrderIterator<TaskId, DefaultEdge> orderIterator = new TopologicalOrderIterator(graph);
        while ( orderIterator.hasNext() )
        {
            TaskId taskId = orderIterator.next();
            Set<DefaultEdge> taskIdEdges = graph.edgesOf(taskId);
            Set<TaskId> processed = taskIdEdges
                .stream()
                .map(graph::getEdgeSource)
                .filter(edge -> !edge.equals(taskId) && !edge.getId().equals(""))
                .collect(Collectors.toSet());
            entries.add(new RunnableTaskDagEntryModel(taskId, processed));
        }
        return new RunnableTaskDagModel(entries);
    }

    private static void worker(DefaultDirectedGraph<TaskId, DefaultEdge> graph, TaskDagModel taskDag, TaskId parentId)
    {
        graph.addVertex(taskDag.getTaskId());
        if ( parentId != null )
        {
            graph.addEdge(parentId, taskDag.getTaskId());
        }
        taskDag.getChildren().forEach(child -> worker(graph, child, taskDag.getTaskId()));
    }
}
