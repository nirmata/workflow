package com.nirmata.workflow.details;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.nirmata.workflow.details.internalmodels.DenormalizedWorkflowModel;
import com.nirmata.workflow.models.RunId;
import com.nirmata.workflow.models.ScheduleId;
import com.nirmata.workflow.models.TaskId;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

import static com.nirmata.workflow.details.InternalJsonSerializer.getDenormalizedWorkflow;
import static com.nirmata.workflow.spi.JsonSerializer.fromBytes;

class Cacher implements Closeable
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final PathChildrenCache runsCache;
    private final ConcurrentMap<RunId, PathChildrenCache> completedTasksCaches;
    private final CuratorFramework curator;
    private final CacherListener cacherListener;
    private final ExecutorService executorService = ThreadUtils.newSingleThreadExecutor("Cacher");

    private final PathChildrenCacheListener scheduleListener = new PathChildrenCacheListener()
    {
        @Override
        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
        {
            switch ( event.getType() )
            {
            default:
            {
                break;  // NOP
            }

            case CHILD_ADDED:
            {
                DenormalizedWorkflowModel workflow = getDenormalizedWorkflow(fromBytes(event.getData().getData()));
                log.debug("New run added: " + workflow);
                PathChildrenCache taskCache = new PathChildrenCache(curator, ZooKeeperConstants.getCompletedTasksParentPath(workflow.getRunId()), false);
                if ( completedTasksCaches.putIfAbsent(workflow.getRunId(), taskCache) == null )
                {
                    taskCache.getListenable().addListener(completedTasksListener, executorService);
                    taskCache.start(PathChildrenCache.StartMode.NORMAL);
                }

                cacherListener.updateAndQueueTasks(Cacher.this, workflow);
                break;
            }

            case CHILD_REMOVED:
            {
                RunId runId = new RunId(ZooKeeperConstants.getRunIdFromRunPath(event.getData().getPath()));
                log.debug("Run removed: " + runId);
                PathChildrenCache cache = completedTasksCaches.remove(runId);
                if ( cache != null )
                {
                    CloseableUtils.closeQuietly(cache);
                }
                break;
            }

            case CHILD_UPDATED:
            {
                DenormalizedWorkflowModel workflow = getDenormalizedWorkflow(fromBytes(event.getData().getData()));
                log.debug("Run updated: " + workflow);
                cacherListener.updateAndQueueTasks(Cacher.this, workflow);
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
                String taskId = ZooKeeperConstants.getTaskIdFromCompletedTaskPath(event.getData().getPath());
                String runId = ZooKeeperConstants.getRunIdFromCompletedTaskPath(event.getData().getPath());
                log.info("Task completed: " + taskId);
                String runPath = ZooKeeperConstants.getRunPath(new RunId(runId));
                ChildData scheduleData = runsCache.getCurrentData(runPath);
                if ( scheduleData == null )
                {
                    String message = "Expected schedule not found at path " + runPath;
                    log.error(message);
                    throw new Exception(message);
                }
                DenormalizedWorkflowModel workflow = getDenormalizedWorkflow(fromBytes(scheduleData.getData()));
                cacherListener.updateAndQueueTasks(Cacher.this, workflow);
            }
        }
    };

    Cacher(CuratorFramework curator, CacherListener cacherListener)
    {
        this.curator = Preconditions.checkNotNull(curator, "curator cannot be null");
        this.cacherListener = Preconditions.checkNotNull(cacherListener, "cacherListener cannot be null");
        runsCache = new PathChildrenCache(curator, ZooKeeperConstants.getRunsParentPath(), true);
        completedTasksCaches = Maps.newConcurrentMap();
    }

    void start()
    {
        runsCache.getListenable().addListener(scheduleListener, executorService);

        try
        {
            runsCache.start(PathChildrenCache.StartMode.NORMAL);
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
        completedTasksCaches.values().forEach(CloseableUtils::closeQuietly);
        CloseableUtils.closeQuietly(runsCache);
    }

    void updateSchedule(DenormalizedWorkflowModel workflow)
    {
        try
        {
            String path = ZooKeeperConstants.getRunPath(workflow.getRunId());
            curator.setData().forPath(path, Scheduler.toJson(log, workflow));
        }
        catch ( Exception e )
        {
            log.error("Could not updateSchedule for workflow: " + workflow, e);
            throw new RuntimeException(e);
        }
    }

    boolean scheduleIsActive(ScheduleId scheduleId)
    {
        Optional<ChildData> any = runsCache
            .getCurrentData()
            .stream()
            .filter(childData -> getDenormalizedWorkflow(fromBytes(childData.getData())).getScheduleId().equals(scheduleId))
            .findAny();
        return any.isPresent();
    }

    boolean taskIsComplete(RunId runId, TaskId taskId)
    {
        PathChildrenCache taskCache = completedTasksCaches.get(runId);
        if ( taskCache != null )
        {
            String path = ZooKeeperConstants.getCompletedTaskPath(runId, taskId);
            return taskCache.getCurrentData(path) != null;
        }
        return false;
    }
}
