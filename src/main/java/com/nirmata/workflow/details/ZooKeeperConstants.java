package com.nirmata.workflow.details;

import com.nirmata.workflow.models.ScheduleId;
import org.apache.curator.utils.ZKPaths;

public class ZooKeeperConstants
{
    public static final String SCHEDULER_LEADER_PATH = "/scheduler-leader";
    public static final String SCHEDULES_PATH = "/schedules";

    public static final int MAX_PAYLOAD = 0xfffff;  // see "jute.maxbuffer" at http://zookeeper.apache.org/doc/r3.3.1/zookeeperAdmin.html

    private ZooKeeperConstants()
    {
    }

    public static String getScheduleKey(ScheduleId scheduleId)
    {
        return ZKPaths.makePath(SCHEDULES_PATH, scheduleId.getId());
    }
}
