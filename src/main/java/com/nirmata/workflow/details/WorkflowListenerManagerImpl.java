/**
 * Copyright 2014 Nirmata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nirmata.workflow.details;

import com.google.common.base.Function;
import com.nirmata.workflow.events.WorkflowEvent;
import com.nirmata.workflow.events.WorkflowListener;
import com.nirmata.workflow.events.WorkflowListenerManager;
import com.nirmata.workflow.models.RunId;
import com.nirmata.workflow.models.TaskId;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.listen.ListenerContainer;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
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

            runsCache.getListenable().addListener(new PathChildrenCacheListener()
            {
                @Override
                public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)
                {
                    RunId runId = new RunId(ZooKeeperConstants.getRunIdFromRunPath(event.getData().getPath()));
                    if ( event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED )
                    {
                        WorkflowListenerManagerImpl.this.postEvent(new WorkflowEvent(WorkflowEvent.EventType.RUN_STARTED, runId));
                    } else if (event.getType() == PathChildrenCacheEvent.Type.CHILD_UPDATED) {
                        WorkflowListenerManagerImpl.this.postEvent(new WorkflowEvent(WorkflowEvent.EventType.RUN_UPDATED, runId));
                    }
                }
            });

            startedTasksCache.getListenable().addListener(new PathChildrenCacheListener()
            {
                @Override
                public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)
                {
                    if ( event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED )
                    {
                        RunId runId = new RunId(ZooKeeperConstants.getRunIdFromStartedTasksPath(event.getData().getPath()));
                        TaskId taskId = new TaskId(ZooKeeperConstants.getTaskIdFromStartedTasksPath(event.getData().getPath()));
                        WorkflowListenerManagerImpl.this.postEvent(new WorkflowEvent(WorkflowEvent.EventType.TASK_STARTED, runId, taskId));
                    }
                }
            });

            completedTasksCache.getListenable().addListener(new PathChildrenCacheListener()
            {
                @Override
                public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)
                {
                    if ( event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED )
                    {
                        RunId runId = new RunId(ZooKeeperConstants.getRunIdFromCompletedTasksPath(event.getData().getPath()));
                        TaskId taskId = new TaskId(ZooKeeperConstants.getTaskIdFromCompletedTasksPath(event.getData().getPath()));
                        WorkflowListenerManagerImpl.this.postEvent(new WorkflowEvent(WorkflowEvent.EventType.TASK_COMPLETED, runId, taskId));
                    }
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

    private void postEvent(final WorkflowEvent event)
    {
        listenerContainer.forEach(new Function<WorkflowListener, Void>()
        {
            @Override
            public Void apply(WorkflowListener l)
            {
                l.receiveEvent(event);
                return null;
            }
        });
    }
}
