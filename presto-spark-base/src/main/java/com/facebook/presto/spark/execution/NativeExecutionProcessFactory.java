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

import com.facebook.airlift.concurrent.BoundedExecutor;
import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.Session;
import com.facebook.presto.client.ServerInfo;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.operator.nativeexecution.ForNativeExecutionTask;
import com.facebook.presto.operator.nativeexecution.NativeExecutionTask;
import com.facebook.presto.server.TaskUpdateRequest;
import com.facebook.presto.sql.planner.PlanFragment;
import io.airlift.units.Duration;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class NativeExecutionProcessFactory
{
    // TODO add config
    private static final int MAX_THREADS = 1000;

    private final HttpClient httpClient;
    private final ExecutorService coreExecutor;
    private final Executor executor;
    private final ScheduledExecutorService errorRetryScheduledExecutor;
    private final ScheduledExecutorService updateScheduledExecutor;
    private final JsonCodec<TaskInfo> taskInfoCodec;
    private final JsonCodec<PlanFragment> planFragmentCodec;
    private final JsonCodec<TaskUpdateRequest> taskUpdateRequestCodec;
    private final JsonCodec<ServerInfo> serverInfoCodec;
    private final TaskManagerConfig taskManagerConfig;

    @Inject
    public NativeExecutionProcessFactory(
            @ForNativeExecutionTask HttpClient httpClient,
            ExecutorService coreExecutor,
            ScheduledExecutorService errorRetryScheduledExecutor,
            ScheduledExecutorService updateScheduledExecutor,
            JsonCodec<TaskInfo> taskInfoCodec,
            JsonCodec<PlanFragment> planFragmentCodec,
            JsonCodec<TaskUpdateRequest> taskUpdateRequestCodec,
            JsonCodec<ServerInfo> serverInfoCodec,
            TaskManagerConfig taskManagerConfig)
    {
        requireNonNull(httpClient, "httpClient is null");
        requireNonNull(coreExecutor, "coreExecutor is null");
        requireNonNull(updateScheduledExecutor, "updateScheduledExecutor is null");
        this.taskInfoCodec = requireNonNull(taskInfoCodec, "taskInfoCodec is null");
        this.planFragmentCodec = requireNonNull(planFragmentCodec, "planFragmentCodec is null");
        this.taskUpdateRequestCodec = requireNonNull(taskUpdateRequestCodec, "taskUpdateRequestCodec is null");
        this.serverInfoCodec = requireNonNull(serverInfoCodec, "serviceInfoCodec is null");
        this.errorRetryScheduledExecutor = requireNonNull(errorRetryScheduledExecutor, "errorRetryScheduledExecutor is null");
        this.httpClient = httpClient;
        this.coreExecutor = coreExecutor;
        this.executor = new BoundedExecutor(coreExecutor, MAX_THREADS);
        this.updateScheduledExecutor = updateScheduledExecutor;
        this.taskManagerConfig = requireNonNull(taskManagerConfig, "taskManagerConfig is null");
    }

    public NativeExecutionTask createNativeExecutionTask(
            Session session,
            URI location,
            TaskId taskId,
            PlanFragment fragment,
            List<TaskSource> sources,
            TableWriteInfo tableWriteInfo)
    {
        return new NativeExecutionTask(
                session,
                location,
                taskId,
                fragment,
                sources,
                httpClient,
                tableWriteInfo,
                new Duration(10, TimeUnit.MINUTES),
                executor,
                errorRetryScheduledExecutor,
                updateScheduledExecutor,
                taskInfoCodec,
                planFragmentCodec,
                taskUpdateRequestCodec,
                serverInfoCodec,
                taskManagerConfig);
    }

    public NativeExecutionProcess createNativeExecutionProcess(
            Session session,
            URI location)
    {
        try {
            return new NativeExecutionProcess(
                    session,
                    location,
                    httpClient,
                    new Duration(120, SECONDS),
                    coreExecutor,
                    errorRetryScheduledExecutor,
                    updateScheduledExecutor,
                    taskInfoCodec,
                    planFragmentCodec,
                    taskUpdateRequestCodec,
                    serverInfoCodec,
                    taskManagerConfig);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @PreDestroy
    public void stop()
    {
        coreExecutor.shutdownNow();
        updateScheduledExecutor.shutdownNow();
    }
}
