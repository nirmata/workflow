package com.nirmata.workflow.details;

import com.nirmata.workflow.admin.WorkflowEvent;
import com.nirmata.workflow.admin.WorkflowListener;
import com.nirmata.workflow.admin.WorkflowListenerManager;
import com.nirmata.workflow.details.WorkflowManagerImpl;
import com.nirmata.workflow.details.ZooKeeperConstants;
import com.nirmata.workflow.models.RunId;
import com.nirmata.workflow.models.TaskId;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.listen.ListenerContainer;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.utils.CloseableUtils;
import java.io.IOException;

public class WorkflowListenerManagerImpl implements WorkflowListenerManager
{
    private final PathChildrenCache completedTasksCache;
    private final PathChildrenCache startedTasksCache;
    private final PathChildrenCache runsCache;
    private final ListenerContainer<WorkflowListener> listenerContainer = new ListenerContainer<>();

    public WorkflowListenerManagerImpl(WorkflowManagerImpl workflowManager)
    {
        completedTasksCache = new PathChildrenCache(workflowManager.getCurator(), ZooKeeperConstants.getCompletedTaskParentPath(), false);
        startedTasksCache = new PathChildrenCache(workflowManager.getCurator(), ZooKeeperConstants.getStartedTasksParentPath(), false);
        runsCache = new PathChildrenCache(workflowManager.getCurator(), ZooKeeperConstants.getRunParentPath(), false);
    }

    @Override
    public void start()
    {
        try
        {
            completedTasksCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
            startedTasksCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
            runsCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);

            runsCache.getListenable().addListener((client, event) -> {
                RunId runId = new RunId(ZooKeeperConstants.getRunIdFromRunPath(event.getData().getPath()));
                if ( event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED )
                {
                    postEvent(new WorkflowEvent(WorkflowEvent.EventType.RUN_STARTED, runId));
                }
                else if ( event.getType() == PathChildrenCacheEvent.Type.CHILD_UPDATED )
                {
                    postEvent(new WorkflowEvent(WorkflowEvent.EventType.RUN_UPDATED, runId));
                }
            });

            startedTasksCache.getListenable().addListener((client, event) -> {
                if ( event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED )
                {
                    RunId runId = new RunId(ZooKeeperConstants.getRunIdFromStartedTasksPath(event.getData().getPath()));
                    TaskId taskId = new TaskId(ZooKeeperConstants.getTaskIdFromStartedTasksPath(event.getData().getPath()));
                    postEvent(new WorkflowEvent(WorkflowEvent.EventType.TASK_STARTED, runId, taskId));
                }
            });

            completedTasksCache.getListenable().addListener((client, event) -> {
                if ( event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED )
                {
                    RunId runId = new RunId(ZooKeeperConstants.getRunIdFromCompletedTasksPath(event.getData().getPath()));
                    TaskId taskId = new TaskId(ZooKeeperConstants.getTaskIdFromCompletedTasksPath(event.getData().getPath()));
                    postEvent(new WorkflowEvent(WorkflowEvent.EventType.TASK_COMPLETED, runId, taskId));
                }
            });
        }
        catch ( Exception e )
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException
    {
        CloseableUtils.closeQuietly(runsCache);
        CloseableUtils.closeQuietly(startedTasksCache);
        CloseableUtils.closeQuietly(completedTasksCache);
    }

    @Override
    public Listenable<WorkflowListener> getListenable()
    {
        return listenerContainer;
    }

    private void postEvent(WorkflowEvent event)
    {
        listenerContainer.forEach(l -> {
            l.receiveEvent(event);
            return null;
        });
    }
}
