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

import com.google.common.base.Preconditions;
import com.nirmata.workflow.admin.AutoCleaner;
import com.nirmata.workflow.admin.WorkflowAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

public class AutoCleanerHolder
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final AutoCleaner autoCleaner;
    private final Duration runPeriod;
    private final AtomicReference<Instant> lastRun = new AtomicReference<>(Instant.now());

    public AutoCleanerHolder(AutoCleaner autoCleaner, Duration runPeriod)
    {
        this.autoCleaner = autoCleaner;
        this.runPeriod = Preconditions.checkNotNull(runPeriod, "runPeriod cannot be null");
    }

    public Duration getRunPeriod()
    {
        return runPeriod;
    }

    public void run(WorkflowAdmin admin)
    {
        Preconditions.checkNotNull(admin, "admin cannot be null");
        log.info("Running");
        if ( autoCleaner != null )
        {
            admin.getRunInfo().stream().filter(autoCleaner::canBeCleaned).forEach(r -> {
                log.info("Auto cleaning: " + r);
                admin.clean(r.getRunId());
            });
        }
        lastRun.set(Instant.now());
    }

    public boolean shouldRun()
    {
        if ( autoCleaner == null )
        {
            return false;
        }
        Duration periodSinceLast = Duration.between(lastRun.get(), Instant.now());
        return (periodSinceLast.compareTo(runPeriod) >= 0);
    }
}
