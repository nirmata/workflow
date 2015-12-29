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
package com.nirmata.workflow.queue.zookeeper.simple;

import com.nirmata.workflow.models.ExecutableTask;
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
    private final boolean idempotent;
    private final ExecutorService executorService = ThreadUtils.newSingleThreadExecutor("SimpleQueue");
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final EnsurePath ensurePath;

    private static final String PREFIX = "qn-";

    SimpleQueue(CuratorFramework client, TaskRunner taskRunner, Serializer serializer, String queuePath, String lockPath, boolean idempotent)
    {
        this.client = client;
        this.taskRunner = taskRunner;
        this.serializer = serializer;
        this.path = queuePath;
        this.lockPath = lockPath;
        this.idempotent = idempotent;
        ensurePath = client.newNamespaceAwareEnsurePath(path);
    }

    void put(byte[] data) throws Exception
    {
        String thisPath = ZKPaths.makePath(path, PREFIX);
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
                    processNode(nodes.get(random.nextInt(nodes.size())));
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
}
