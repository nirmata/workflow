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

import com.google.common.base.Preconditions;
import com.nirmata.workflow.models.RunId;
import java.time.LocalDateTime;
import java.util.Optional;

/**
 * Run information
 */
public class RunInfo
{
    private final RunId runId;
    private final LocalDateTime startTimeUtc;
    private final Optional<LocalDateTime> completionTimeUtc;

    public RunInfo(RunId runId, LocalDateTime startTimeUtc)
    {
        this(runId, startTimeUtc, null);
    }

    public RunInfo(RunId runId, LocalDateTime startTimeUtc, LocalDateTime completionTimeUtc)
    {
        this.runId = Preconditions.checkNotNull(runId, "runId cannot be null");
        this.startTimeUtc = Preconditions.checkNotNull(startTimeUtc, "startTimeUtc cannot be null");
        this.completionTimeUtc = Optional.ofNullable(completionTimeUtc);
    }

    public RunId getRunId()
    {
        return runId;
    }

    public LocalDateTime getStartTimeUtc()
    {
        return startTimeUtc;
    }

    public LocalDateTime getCompletionTimeUtc()
    {
        //noinspection ConstantConditions
        return completionTimeUtc.get(); // exception if empty is desired
    }

    public boolean isComplete()
    {
        return completionTimeUtc.isPresent();
    }

    @Override
    public boolean equals(Object o)
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        RunInfo runInfo = (RunInfo)o;

        if ( !completionTimeUtc.equals(runInfo.completionTimeUtc) )
        {
            return false;
        }
        if ( !runId.equals(runInfo.runId) )
        {
            return false;
        }
        //noinspection RedundantIfStatement
        if ( !startTimeUtc.equals(runInfo.startTimeUtc) )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = runId.hashCode();
        result = 31 * result + startTimeUtc.hashCode();
        result = 31 * result + completionTimeUtc.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return "RunInfo{" +
            "runId=" + runId +
            ", startTime=" + startTimeUtc +
            ", completionTime=" + completionTimeUtc +
            '}';
    }
}
