
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
package com.facebook.presto.operator;

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.client.ServerInfo;
import com.facebook.presto.operator.nativeexecution.PrestoSparkHttpServerClient;
import com.facebook.presto.server.RequestErrorTracker;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.Duration;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class NativeExecutionProcess
{
    private static final Logger log = Logger.get(NativeExecutionProcess.class);
    private Process process;
    private final Session session;
    private final PrestoSparkHttpServerClient workerClient;

    public NativeExecutionProcess(
            Session session, URI uri, HttpClient httpClient, Duration maxErrorDuration, ScheduledExecutorService scheduledExecutor, JsonCodec<ServerInfo> serverInfoCodec)
    {
        this.session = requireNonNull(session, "session is null");
        this.workerClient = new PrestoSparkHttpServerClient(httpClient, uri, serverInfoCodec, maxErrorDuration, scheduledExecutor);
    }

    public void close()
    {

        if (process.isAlive()) {
            process.destroy();
        }
    }

    /**
     * Starts the external native execution process. Any exceptional cases during the process bootstrap should be captured by the exception handling mechanism of CompletableFuture.
     *
     * @return a CompletableFuture of no content to indicate the successful finish of the task.
     */
    public void start()
            throws ExecutionException, InterruptedException
    {
        String executablePath = SystemSessionProperties.getNativeExecutionExecutablePath(session);

        ProcessBuilder processBuilder = new ProcessBuilder(executablePath, "--log_dir", "/Users/mjdeng/", "--v", "1", "--etc_dir", "/Users/mjdeng/Projects/presto/presto-native-execution/etc");
        try {
            process = processBuilder.start();
            Thread.sleep(10000);
        }
        catch (IOException | InterruptedException e) {
            throw new RuntimeException(format("Cannot start %s", processBuilder.command()), e);
        }

        workerClient.getServerInfoWithRetry().get();
    }

    private void doCheckNativeProcess(RequestErrorTracker errorTracker, Request request, SettableFuture<?> future)
    {
        errorTracker.startRequest();
    }
}
