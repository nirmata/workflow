package com.nirmata.workflow.details;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.nirmata.workflow.WorkflowManager;
import com.nirmata.workflow.details.internalmodels.DenormalizedWorkflowModel;
import com.nirmata.workflow.models.ScheduleId;
import com.nirmata.workflow.models.TaskId;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.CloseableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.util.concurrent.ConcurrentMap;

import static com.nirmata.workflow.details.InternalJsonSerializer.getDenormalizedWorkflow;
import static com.nirmata.workflow.spi.JsonSerializer.fromBytes;

class Cacher implements Closeable
{
    private final Logger log = LoggerFactory.getLogger(getClass());
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
                ScheduleId scheduleId = new ScheduleId(ZooKeeperConstants.getScheduleIdFromSchedulePath(event.getData().getPath()));
                PathChildrenCache taskCache = new PathChildrenCache(workflowManager.getCurator(), ZooKeeperConstants.getCompletedTasksParentPath(scheduleId), true);
                if ( completedTasksCache.putIfAbsent(scheduleId, taskCache) == null )
                {
                    taskCache.getListenable().addListener(completedTasksListener);
                    taskCache.start(PathChildrenCache.StartMode.NORMAL);
                }

                DenormalizedWorkflowModel workflow = getDenormalizedWorkflow(fromBytes(event.getData().getData()));
                cacherListener.updateAndQueueTasks(Cacher.this, workflow);
            }
            else if ( event.getType() == PathChildrenCacheEvent.Type.CHILD_REMOVED )
            {
                ScheduleId scheduleId = new ScheduleId(ZooKeeperConstants.getScheduleIdFromSchedulePath(event.getData().getPath()));
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
        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
        {
            if ( event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED )
            {
                String scheduleIdKey = ZooKeeperConstants.getScheduleIdFromCompletedTaskPath(event.getData().getPath());
                ChildData scheduleData = scheduleCache.getCurrentData(scheduleIdKey);
                if ( scheduleData == null )
                {
                    String message = "Expected schedule not found at path " + scheduleIdKey;
                    log.error(message);
                    throw new Exception(message);
                }
                DenormalizedWorkflowModel workflow = getDenormalizedWorkflow(fromBytes(scheduleData.getData()));
                cacherListener.updateAndQueueTasks(Cacher.this, workflow);
            }
        }
    };

    Cacher(WorkflowManager workflowManager, CacherListener cacherListener)
    {
        this.cacherListener = cacherListener;
        this.workflowManager = Preconditions.checkNotNull(workflowManager, "workflowManager cannot be null");
        scheduleCache = new PathChildrenCache(workflowManager.getCurator(), ZooKeeperConstants.getScheduleParentPath(), true);
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
            log.error("Could not start schedule cache", e);
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
        return scheduleCache.getCurrentData(ZooKeeperConstants.getSchedulePath(scheduleId)) != null;
    }

    public ChildData getCompletedData(ScheduleId scheduleId, TaskId taskId)
    {
        PathChildrenCache taskCache = completedTasksCache.get(scheduleId);
        if ( taskCache != null )
        {
            String path = ZooKeeperConstants.getCompletedTaskPath(scheduleId, taskId);
            return taskCache.getCurrentData(path);
        }
        return null;
    }
}
