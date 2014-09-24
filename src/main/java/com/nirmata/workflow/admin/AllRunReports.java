package com.nirmata.workflow.admin;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.nirmata.workflow.details.ZooKeeperConstants;
import com.nirmata.workflow.models.RunId;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class AllRunReports
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Map<RunId, RunReport> reports;

    public AllRunReports(CuratorFramework curator)
    {
        Set<RunId> runIds = Sets.newHashSet();
        getAllRunIds(curator, ZooKeeperConstants.getCompletedRunParentPath(), ZooKeeperConstants::getRunIdFromCompletedTasksPath, runIds);
        getAllRunIds(curator, ZooKeeperConstants.getRunsParentPath(), ZooKeeperConstants::getRunIdFromRunPath, runIds);

        ImmutableMap.Builder<RunId, RunReport> builder = ImmutableMap.builder();
        runIds.forEach(runId -> builder.put(runId, new RunReport(curator, runId)));
        reports = builder.build();
    }

    public Map<RunId, RunReport> getReports()
    {
        return reports;
    }

    private void getAllRunIds(CuratorFramework curator, String path, Function<String, String> getId, Set<RunId> ids)
    {
        try
        {
            List<String> strings = curator.getChildren().forPath(path);
            for ( String s : strings )
            {
                String id = getId.apply(ZKPaths.makePath(path, s));
                ids.add(new RunId(id));
            }
        }
        catch ( KeeperException.NoNodeException ignore )
        {
            // ignore
        }
        catch ( Exception e )
        {
            String message = "Could not read from path: " + path;
            log.error(message, e);
            throw new RuntimeException(message, e);
        }
    }
}
