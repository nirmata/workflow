package com.nirmata.workflow.admin;

import com.nirmata.workflow.details.ZooKeeperConstants;
import com.nirmata.workflow.models.RunId;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.function.Predicate;

public class Cleaner
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final CuratorFramework curator;

    public Cleaner(CuratorFramework curator)
    {
        this.curator = curator;
    }

    public void clean(RunId runId)
    {
        internalClean(ZooKeeperConstants.getStartedTasksParentPath(), p -> runId.getId().equals(ZooKeeperConstants.getRunIdFromCompletedTasksPath(p)));
        internalClean(ZooKeeperConstants.getCompletedTasksParentPath(), p -> runId.getId().equals(ZooKeeperConstants.getRunIdFromStartedTasksPath(p)));
        internalClean(ZooKeeperConstants.getRunsParentPath(), p -> runId.getId().equals(ZooKeeperConstants.getRunIdFromRunPath(p)));
        internalClean(ZooKeeperConstants.getCompletedRunParentPath(), p -> runId.getId().equals(ZooKeeperConstants.getRunIdFromCompletedRunPath(p)));
    }

    private void internalClean(String path, Predicate<String> isCleanable)
    {
        try
        {
            List<String> children = curator.getChildren().forPath(path);
            children.stream().map(child -> ZKPaths.makePath(path, child)).filter(isCleanable).forEach(deletePath -> {
                try
                {
                    curator.delete().guaranteed().forPath(deletePath);
                }
                catch ( KeeperException.NoNodeException ignore )
                {
                    // ignore
                }
                catch ( Exception e )
                {
                    String message = "Could not delete path: " + deletePath;
                    log.error(message, e);
                    throw new RuntimeException(e);
                }
            });
        }
        catch ( KeeperException.NoNodeException ignore )
        {
            // ignore
        }
        catch ( Exception e )
        {
            String message = "Could not read from path: " + path;
            log.error(message, e);
            throw new RuntimeException(e);
        }
    }
}
