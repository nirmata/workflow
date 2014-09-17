package com.nirmata.workflow;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.nirmata.workflow.models.TaskId;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ConcurrentTaskChecker
{
    private final Set<TaskId> currentSet = Sets.newHashSet();
    private final List<TaskId> all = Lists.newArrayList();
    private int count = 0;
    private final List<Set<TaskId>> sets = Lists.newArrayList();

    public synchronized void add(TaskId taskId)
    {
        all.add(taskId);
        currentSet.add(taskId);
        ++count;
    }

    public synchronized void decrement()
    {
        if ( --count == 0 )
        {
            HashSet<TaskId> copy = Sets.newHashSet(currentSet);
            currentSet.clear();
            count = 0;
            sets.add(copy);
        }
    }

    public synchronized List<Set<TaskId>> getSets()
    {
        return Lists.newArrayList(sets);
    }

    public synchronized List<TaskId> getAll()
    {
        return Lists.newArrayList(all);
    }
}
