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
package com.nirmata.workflow.admin;

import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;

/**
 * Default auto cleaner. Cleans after a quiet period following run completion.
 */
public class StandardAutoCleaner implements AutoCleaner
{
    private final Duration quietPeriod;

    /**
     * @return after a run is complete, how long to wait before cleaning it.
     */
    public StandardAutoCleaner(Duration quietPeriod)
    {
        this.quietPeriod = quietPeriod;
    }

    @Override
    public boolean canBeCleaned(RunInfo runInfo)
    {
        if ( runInfo.isComplete() )
        {
            LocalDateTime nowUtc = LocalDateTime.now(Clock.systemUTC());
            Duration durationSinceCompletion = Duration.between(runInfo.getCompletionTimeUtc(), nowUtc);
            if ( durationSinceCompletion.compareTo(quietPeriod) >= 0 )
            {
                return true;
            }
        }
        return false;
    }
}
