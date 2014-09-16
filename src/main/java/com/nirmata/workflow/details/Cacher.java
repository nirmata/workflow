package com.nirmata.workflow.details;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.nirmata.workflow.WorkflowManager;
import com.nirmata.workflow.details.internalmodels.DenormalizedWorkflowModel;
import com.nirmata.workflow.models.ScheduleId;
import com.nirmata.workflow.spi.JsonSerializer;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.CloseableUtils;
import java.io.Closeable;
import java.util.concurrent.ConcurrentMap;

class Cacher implements Closeable
{
    private final WorkflowManager workflowManager;
    private final PathChildrenCache scheduleCache;
    private final ConcurrentMap<ScheduleId, PathChildrenCache> completedTasksCache;
    private final CacherListener cacherListener;

    private final PathChildrenCacheListener scheduleListener = new PathChildrenCacheListener()
    {
        @Override
        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
        {
            if ( event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED )
            {
                ScheduleId scheduleId = new ScheduleId(ZooKeeperConstants.getScheduleIdFromScheduleKey(event.getData().getPath()));
                PathChildrenCache taskCache = new PathChildrenCache(workflowManager.getCurator(), ZooKeeperConstants.getCompletedTasksKey(scheduleId), true);
                if ( completedTasksCache.putIfAbsent(scheduleId, taskCache) == null )
                {
                    taskCache.getListenable().addListener(completedTasksListener);
                    taskCache.start(PathChildrenCache.StartMode.NORMAL);
                }

                DenormalizedWorkflowModel workflow = InternalJsonSerializer.getDenormalizedWorkflow(JsonSerializer.fromBytes(event.getData().getData()));
                cacherListener.updateAndQueueTasks(workflow);
            }
            else if ( event.getType() == PathChildrenCacheEvent.Type.CHILD_REMOVED )
            {
                ScheduleId scheduleId = new ScheduleId(ZooKeeperConstants.getScheduleIdFromScheduleKey(event.getData().getPath()));
                PathChildrenCache cache = completedTasksCache.remove(scheduleId);
                if ( cache != null )
                {
                    CloseableUtils.closeQuietly(cache);
                }
            }
        }
    };

    private final PathChildrenCacheListener completedTasksListener = new PathChildrenCacheListener()
    {
        @Override
        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)
        {
            if ( event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED )
            {
                ChildData scheduleData = scheduleCache.getCurrentData(ZooKeeperConstants.getScheduleIdKeyFromCompletedTaskPath(event.getData().getPath()));
                if ( scheduleData == null )
                {
                    // TODO
                }
                DenormalizedWorkflowModel workflow = InternalJsonSerializer.getDenormalizedWorkflow(JsonSerializer.fromBytes(scheduleData.getData()));
                cacherListener.updateAndQueueTasks(workflow);
            }
        }
    };

    Cacher(WorkflowManager workflowManager, CacherListener cacherListener)
    {
        this.cacherListener = cacherListener;
        this.workflowManager = Preconditions.checkNotNull(workflowManager, "workflowManager cannot be null");
        scheduleCache = new PathChildrenCache(workflowManager.getCurator(), ZooKeeperConstants.SCHEDULES_PATH, true);
        completedTasksCache = Maps.newConcurrentMap();
    }

    public void start()
    {
        scheduleCache.getListenable().addListener(scheduleListener);

        try
        {
            scheduleCache.start(PathChildrenCache.StartMode.NORMAL);
        }
        catch ( Exception e )
        {
            // TODO log
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close()
    {
        for ( PathChildrenCache cache : completedTasksCache.values() )
        {
            CloseableUtils.closeQuietly(cache);
        }
        CloseableUtils.closeQuietly(scheduleCache);
    }

    public boolean scheduleIsActive(ScheduleId scheduleId)
    {
        return scheduleCache.getCurrentData(ZooKeeperConstants.getScheduleKey(scheduleId)) != null;
    }
}
