package com.nirmata.workflow.details;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.nirmata.workflow.details.internalmodels.RunnableTaskDag;
import com.nirmata.workflow.models.Task;
import com.nirmata.workflow.models.TaskId;
import org.jgrapht.alg.CycleDetector;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.traverse.TopologicalOrderIterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class RunnableTaskDagBuilder
{
    private final List<RunnableTaskDag> entries;
    private final Map<TaskId, Task> tasks;

    public RunnableTaskDagBuilder(Task task)
    {
        ImmutableList.Builder<RunnableTaskDag> entriesBuilder = ImmutableList.builder();
        ImmutableMap.Builder<TaskId, Task> tasksBuilder = ImmutableMap.builder();
        build(task, entriesBuilder, tasksBuilder);

        entries = entriesBuilder.build();
        tasks = tasksBuilder.build();
    }

    public List<RunnableTaskDag> getEntries()
    {
        return entries;
    }

    public Map<TaskId, Task> getTasks()
    {
        return tasks;
    }

    private void build(Task task, ImmutableList.Builder<RunnableTaskDag> entriesBuilder, ImmutableMap.Builder<TaskId, Task> tasksBuilder)
    {
        DefaultDirectedGraph<TaskId, DefaultEdge> graph = new DefaultDirectedGraph<>(DefaultEdge.class);
        worker(graph, task, null, tasksBuilder, Sets.newHashSet());

        CycleDetector<TaskId, DefaultEdge> cycleDetector = new CycleDetector<>(graph);
        if ( cycleDetector.detectCycles() )
        {
            throw new RuntimeException("The Task DAG contains cycles: " + task);
        }

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
            entriesBuilder.add(new RunnableTaskDag(taskId, processed));
        }
    }

    private void worker(DefaultDirectedGraph<TaskId, DefaultEdge> graph, Task task, TaskId parentId, ImmutableMap.Builder<TaskId, Task> tasksBuilder, Set<TaskId> usedTasksSet)
    {
        if ( usedTasksSet.add(task.getTaskId()) )
        {
            tasksBuilder.put(task.getTaskId(), task);
        }

        graph.addVertex(task.getTaskId());
        if ( parentId != null )
        {
            graph.addEdge(parentId, task.getTaskId());
        }
        task.getChildrenTasks().forEach(child -> worker(graph, child, task.getTaskId(), tasksBuilder, usedTasksSet));
    }
}
