package com.nirmata.workflow.details;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.nirmata.workflow.details.internalmodels.RunnableTaskDag;
import com.nirmata.workflow.models.ExecutableTask;
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
    private final Map<TaskId, ExecutableTask> tasks;

    public RunnableTaskDagBuilder(Task task)
    {
        ImmutableList.Builder<RunnableTaskDag> entriesBuilder = ImmutableList.builder();
        ImmutableMap.Builder<TaskId, ExecutableTask> tasksBuilder = ImmutableMap.builder();
        build(task, entriesBuilder, tasksBuilder);

        entries = entriesBuilder.build();
        tasks = tasksBuilder.build();
    }

    public List<RunnableTaskDag> getEntries()
    {
        return entries;
    }

    public Map<TaskId, ExecutableTask> getTasks()
    {
        return tasks;
    }

    private void build(Task task, ImmutableList.Builder<RunnableTaskDag> entriesBuilder, ImmutableMap.Builder<TaskId, ExecutableTask> tasksBuilder)
    {
        DefaultDirectedGraph<TaskId, DefaultEdge> graph = new DefaultDirectedGraph<>(DefaultEdge.class);
        worker(graph, task, null, tasksBuilder);

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

    private static void worker(DefaultDirectedGraph<TaskId, DefaultEdge> graph, Task task, TaskId parentId, ImmutableMap.Builder<TaskId, ExecutableTask> tasksBuilder)
    {
        tasksBuilder.put(task.getTaskId(), new ExecutableTask(task.getTaskId(), task.getTaskType(), task.getMetaData(), task.isExecutable()));

        graph.addVertex(task.getTaskId());
        if ( parentId != null )
        {
            graph.addEdge(parentId, task.getTaskId());
        }
        task.getChildrenTasks().forEach(child -> worker(graph, child, task.getTaskId(), tasksBuilder));
    }
}
