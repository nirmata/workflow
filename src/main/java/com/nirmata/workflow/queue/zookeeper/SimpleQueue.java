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
package com.nirmata.workflow.queue.zookeeper;

import com.nirmata.workflow.models.ExecutableTask;
import com.nirmata.workflow.models.TaskMode;
import com.nirmata.workflow.queue.QueueConsumer;
import com.nirmata.workflow.queue.TaskRunner;
import com.nirmata.workflow.serialization.Serializer;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.EnsurePath;
import org.apache.curator.utils.ThreadUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

// copied and modified from org.apache.curator.framework.recipes.queue.SimpleDistributedQueue
class SimpleQueue implements Closeable, QueueConsumer
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final CuratorFramework client;
    private final TaskRunner taskRunner;
    private final Serializer serializer;
    private final String path;
    private final String lockPath;
    private final TaskMode mode;
    private final boolean idempotent;
    private final ExecutorService executorService = ThreadUtils.newSingleThreadExecutor("SimpleQueue");
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final EnsurePath ensurePath;

    private static final String PREFIX = "qn-";
    private static final String SEPARATOR = "|";

    SimpleQueue(CuratorFramework client, TaskRunner taskRunner, Serializer serializer, String queuePath, String lockPath, TaskMode mode, boolean idempotent)
    {
        this.client = client;
        this.taskRunner = taskRunner;
        this.serializer = serializer;
        this.path = queuePath;
        this.lockPath = lockPath;
        this.mode = mode;
        this.idempotent = idempotent;
        ensurePath = client.newNamespaceAwareEnsurePath(path);
    }

    void put(byte[] data, long value) throws Exception
    {
        String thisPath = ZKPaths.makePath(path, PREFIX);
        if ( mode == TaskMode.PRIORITY )
        {
            String priorityHex = priorityToString(value);
            thisPath += priorityHex;
        }
        else if ( mode == TaskMode.DELAY )
        {
            thisPath += epochToString(value);
        }
        client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT_SEQUENTIAL).inBackground().forPath(thisPath, data);
    }

    public void start()
    {
        if ( started.compareAndSet(false, true) )
        {
            executorService.submit(this::runLoop);
        }
    }

    @Override
    public void close()
    {
        if ( started.compareAndSet(true, false) )
        {
            executorService.shutdownNow();
        }
    }

    private void runLoop()
    {
        log.info("Starting runLoop");

        Random random = new Random();
        while ( started.get() && !Thread.currentThread().isInterrupted() )
        {
            try
            {
                ensurePath.ensure(client.getZookeeperClient());

                CountDownLatch latch = new CountDownLatch(1);
                Watcher watcher = event -> latch.countDown();
                List<String> nodes = client.getChildren().usingWatcher(watcher).forPath(path);
                if ( nodes.size() == 0 )
                {
                    latch.await();
                }
                else
                {
                    switch ( mode )
                    {
                        case STANDARD:
                        {
                            String node = nodes.get(random.nextInt(nodes.size()));
                            processNode(node);
                            break;
                        }

                        case DELAY:
                        {
                            long sortTime = System.currentTimeMillis();
                            String node = getDelayNode(nodes, sortTime);
                            long delay = getDelay(node, sortTime);
                            if ( delay > 0 )
                            {
                                latch.await(delay, TimeUnit.MILLISECONDS);
                            }
                            else
                            {
                                processNode(node);
                            }
                            break;
                        }

                        case PRIORITY:
                        {
                            Collections.sort(nodes);
                            processNode(nodes.get(0));
                            break;
                        }
                    }
                }
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
                break;
            }
            catch ( Exception e )
            {
                log.error("Could not process queue", e);
            }
        }
        log.info("Exiting runLoop");
    }

    private long getDelay(String itemNode, long sortTime)
    {
        long epoch = getEpoch(itemNode);
        return epoch - sortTime;
    }

    private String getDelayNode(List<String> nodes, long sortTime)
    {
        Collections.sort(nodes, (s1, s2) -> {
            long        diff = getDelay(s1, sortTime) - getDelay(s2, sortTime);
            return (diff < 0) ? -1 : ((diff > 0) ? 1 : 0);
        });
        return nodes.get(0);
    }

    private void processNode(String node) throws Exception
    {
        String lockNodePath = ZKPaths.makePath(lockPath, node);
        boolean lockCreated = false;
        try
        {
            try
            {
                if ( idempotent )
                {
                    client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(lockNodePath);
                    lockCreated = true;
                }

                String itemPath = ZKPaths.makePath(this.path, node);
                Stat stat = new Stat();
                byte[] bytes = client.getData().storingStatIn(stat).forPath(itemPath);
                ExecutableTask executableTask = serializer.deserialize(bytes, ExecutableTask.class);
                try
                {
                    if ( !idempotent )
                    {
                        client.delete().withVersion(stat.getVersion()).forPath(itemPath);
                    }
                    taskRunner.executeTask(executableTask);
                    if ( idempotent )
                    {
                        client.delete().guaranteed().forPath(itemPath);
                    }
                }
                catch ( Throwable e )
                {
                    log.error("Exception processing task at: " + itemPath, e);
                }
            }
            catch ( KeeperException.NodeExistsException ignore )
            {
                // another process got it
            }
        }
        catch ( KeeperException.NoNodeException | KeeperException.BadVersionException ignore )
        {
            // another process got this node already - ignore
        }
        finally
        {
            if ( lockCreated )
            {
                client.delete().guaranteed().forPath(lockNodePath);
            }
        }
    }

    private static String priorityToString(long priority)
    {
        // the padded hex val of the number prefixed with a 0 for negative numbers
        // and a 1 for positive (so that it sorts correctly)
        long        l = priority & 0xFFFFFFFFL;
        return String.format("%s%08X", (priority >= 0) ? "1" : "0", l);
    }

    private static String epochToString(long epoch)
    {
        return SEPARATOR + String.format("%08X", epoch) + SEPARATOR;
    }

    private static long getEpoch(String itemNode)
    {
        int     index2 = itemNode.lastIndexOf(SEPARATOR);
        int     index1 = (index2 > 0) ? itemNode.lastIndexOf(SEPARATOR, index2 - 1) : -1;
        if ( (index1 > 0) && (index2 > (index1 + 1)) )
        {
            try
            {
                String  epochStr = itemNode.substring(index1 + 1, index2);
                return Long.parseLong(epochStr, 16);
            }
            catch ( NumberFormatException ignore )
            {
                // ignore
            }
        }
        return 0;
    }
}
