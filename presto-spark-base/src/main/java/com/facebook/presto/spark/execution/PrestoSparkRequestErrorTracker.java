/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.spark.execution;

import com.facebook.presto.server.RequestErrorTracker;
import com.facebook.presto.spi.ErrorCodeSupplier;
import io.airlift.units.Duration;

import javax.annotation.concurrent.ThreadSafe;

import java.net.URI;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.presto.spi.StandardErrorCode.NATIVE_EXECUTION_TASK_ERROR;

@ThreadSafe
public class PrestoSparkRequestErrorTracker
        extends RequestErrorTracker
{
    private static final String NATIVE_EXECUTION_TASK_ERROR_MESSAGE = "Encountered too many errors talking to native process. The process may have crashed or be under too much load.";

    protected PrestoSparkRequestErrorTracker(
            Object id,
            URI uri,
            ErrorCodeSupplier errorCode,
            String nodeErrorMessage,
            Duration maxErrorDuration,
            ScheduledExecutorService scheduledExecutor,
            String jobDescription)
    {
        super(id, uri, errorCode, nodeErrorMessage, maxErrorDuration, scheduledExecutor, jobDescription);
    }

    public static PrestoSparkRequestErrorTracker nativeExecutionRequestErrorTracker(String nodeId, URI taskUri, Duration maxErrorDuration, ScheduledExecutorService scheduledExecutor, String jobDescription)
    {
        return new PrestoSparkRequestErrorTracker(nodeId, taskUri, NATIVE_EXECUTION_TASK_ERROR, NATIVE_EXECUTION_TASK_ERROR_MESSAGE, maxErrorDuration, scheduledExecutor, jobDescription);
    }
}
