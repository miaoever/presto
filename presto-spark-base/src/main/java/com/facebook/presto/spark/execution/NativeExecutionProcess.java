
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
import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.client.ServerInfo;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.operator.common.FilePropertiesPopulator;
import com.facebook.presto.operator.common.Properties;
import com.facebook.presto.operator.common.WorkerConfigProperties;
import com.facebook.presto.operator.common.WorkerHiveConnectorConfigProperties;
import com.facebook.presto.operator.common.WorkerNodeProperties;
import com.facebook.presto.operator.nativeexecution.NativeExecutionTask;
import com.facebook.presto.operator.nativeexecution.PrestoSparkHttpServerClient;
import com.facebook.presto.server.TaskUpdateRequest;
import com.facebook.presto.sql.planner.PlanFragment;
import io.airlift.units.Duration;
import org.apache.spark.SparkFiles;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.presto.SystemSessionProperties.getNativeExecutionCatalogName;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class NativeExecutionProcess
{
    private static final Logger log = Logger.get(NativeExecutionProcess.class);
    private static final int MAX_THREADS = 1000;
    private static final int NATIVE_PROCESS_DEFAULT_PORT = 7777;
    private static final String SERVER_URI = "/v1/info";
    private static final String TASK_URI = "/v1/task/";
    private static final String WORKER_CONFIG_FILE = "/config.properties";
    private static final String WORKER_NODE_CONFIG_FILE = "/node.properties";
    private static final String WORKER_CATALOG_CONFIG_FILE = "/catalog/";
    private Process process;
    private final Session session;
    private final PrestoSparkHttpServerClient workerClient;
    private final HttpClient httpClient;
    private final ExecutorService coreExecutor;
    private final Executor executor;
    private final URI location;
    private final ScheduledExecutorService updateScheduledExecutor;
    private final JsonCodec<TaskInfo> taskInfoCodec;
    private final JsonCodec<PlanFragment> planFragmentCodec;
    private final JsonCodec<TaskUpdateRequest> taskUpdateRequestCodec;
    private final JsonCodec<ServerInfo> serverInfoCodec;
    private final int port;
    private final ScheduledExecutorService errorRetryScheduledExecutor;
    private final TaskManagerConfig taskManagerConfig;
    private NativeExecutionTask nativeExecutionTask;

    public NativeExecutionProcess(
            Session session,
            URI uri,
            HttpClient httpClient,
            Duration maxErrorDuration,
            ExecutorService coreExecutor,
            ScheduledExecutorService errorRetryScheduledExecutor,
            ScheduledExecutorService updateScheduledExecutor,
            JsonCodec<TaskInfo> taskInfoCodec,
            JsonCodec<PlanFragment> planFragmentCodec,
            JsonCodec<TaskUpdateRequest> taskUpdateRequestCodec,
            JsonCodec<ServerInfo> serverInfoCodec,
            TaskManagerConfig taskManagerConfig)
            throws IOException
    {
        this.port = getAvailableTcpPort();
        this.session = requireNonNull(session, "session is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.location = getBaseUriWithPort(requireNonNull(uri, "uri is null"), port);
        this.workerClient = new PrestoSparkHttpServerClient(httpClient, getServerUri(location), serverInfoCodec, maxErrorDuration, errorRetryScheduledExecutor);
        this.taskInfoCodec = requireNonNull(taskInfoCodec, "taskInfoCodec is null");
        this.planFragmentCodec = requireNonNull(planFragmentCodec, "planFragmentCodec is null");
        this.taskUpdateRequestCodec = requireNonNull(taskUpdateRequestCodec, "taskUpdateRequestCodec is null");
        this.serverInfoCodec = requireNonNull(serverInfoCodec, "serviceInfoCodec is null");
        this.taskManagerConfig = requireNonNull(taskManagerConfig, "taskManagerConfig is null");
        this.coreExecutor = coreExecutor;
        this.executor = new BoundedExecutor(coreExecutor, MAX_THREADS);
        this.updateScheduledExecutor = updateScheduledExecutor;
        this.errorRetryScheduledExecutor = errorRetryScheduledExecutor;
    }

    public void close()
    {
        if (process.isAlive()) {
            process.destroy();
        }
    }

    private URI getServerUri(URI baseUri)
    {
        return uriBuilderFrom(baseUri)
                .appendPath(SERVER_URI)
                .build();
    }

    private URI getTaskUri(URI baseUri)
    {
        return uriBuilderFrom(baseUri)
                .appendPath(TASK_URI)
                .build();
    }

    private URI getBaseUriWithPort(URI baseUri, int port)
    {
        return uriBuilderFrom(baseUri)
                .port(port)
                .build();
    }

    private int getAvailableTcpPort()
            throws IOException
    {
        int port = NATIVE_PROCESS_DEFAULT_PORT;
        ServerSocket socket = new ServerSocket(0);
        port = socket.getLocalPort();
        socket.close();
        return port;
    }

    private void populateConfigurationFiles(String configBasePath)
    {
        WorkerConfigProperties.WorkerConfigPropertiesBuilder configPropertiesBuilder = new WorkerConfigProperties.WorkerConfigPropertiesBuilder();
        configPropertiesBuilder.initializeDefault().setPopulator(new FilePropertiesPopulator(configBasePath + WORKER_CONFIG_FILE));
        configPropertiesBuilder.setProperty(WorkerConfigProperties.HTTP_SERVER_HTTP_PORT, String.valueOf(port));

        WorkerNodeProperties.WorkerNodePropertiesBuilder nodePropertiesBuilder = new WorkerNodeProperties.WorkerNodePropertiesBuilder();
        nodePropertiesBuilder.initializeDefault().setPopulator(new FilePropertiesPopulator(configBasePath + WORKER_NODE_CONFIG_FILE));

        WorkerHiveConnectorConfigProperties.WorkerHiveConnectorConfigPropertiesBuilder connectorPropertiesBuilder = new WorkerHiveConnectorConfigProperties.WorkerHiveConnectorConfigPropertiesBuilder();
        connectorPropertiesBuilder.initializeDefault().setPopulator(
                new FilePropertiesPopulator(configBasePath + format("%s%s.properties", WORKER_CATALOG_CONFIG_FILE, getNativeExecutionCatalogName(session))));

        System.getenv().forEach((k, v) -> {
            if (k.startsWith("PRESTO_SPARK_WORKER_PROPERTIES.")) {
                String key = k.substring("PRESTO_SPARK_WORKER_PROPERTIES.".length());
                configPropertiesBuilder.setProperty(key, v);
                log.info("Overwriting worker config %s=%s", key, v);
            }
            else if (k.startsWith("PRESTO_SPARK_NODE_PROPERTIES.")) {
                String key = k.substring("PRESTO_SPARK_NODE_PROPERTIES.".length());
                nodePropertiesBuilder.setProperty(key, v);
                log.info("Overwriting node config %s=%s", key, v);
            }
            else if (k.startsWith("PRESTO_SPARK_HIVE_CATALOG_PROPERTIES.")) {
                String key = k.substring("PRESTO_SPARK_HIVE_CATALOG_PROPERTIES.".length());
                connectorPropertiesBuilder.setProperty(key, v);
                log.info("Overwriting hive catalog config %s=%s", key, v);
            }
        });

        Properties workerConfigProperties = configPropertiesBuilder.build();
        Properties workerNodeProperties = nodePropertiesBuilder.build();
        Properties hiveConnectorProperties = connectorPropertiesBuilder.build();
        workerConfigProperties.populateProperties();
        workerNodeProperties.populateProperties();
        hiveConnectorProperties.populateProperties();
    }

    public String getProcessWorkingPath(String path)
    {
        File absolutePath = new File(path);
        File workingDir = new File(SparkFiles.getRootDirectory());
        if (!absolutePath.isAbsolute()) {
            absolutePath = new File(workingDir, path);
        }

        if (!absolutePath.exists()) {
            throw new RuntimeException(format("File doesn't exist %s", absolutePath.getAbsolutePath()));
        }

        return absolutePath.getAbsolutePath();
    }

    /**
     * Starts the external native execution process. Any exceptional cases during the process bootstrap should be captured by the exception handling mechanism of CompletableFuture.
     *
     * @return a CompletableFuture of no content to indicate the successful finish of the task.
     */
    public void start()
            throws ExecutionException, InterruptedException, IOException
    {
        String executablePath = getProcessWorkingPath(SystemSessionProperties.getNativeExecutionExecutablePath(session));
        String logPath = getProcessWorkingPath(SystemSessionProperties.getNativeExecutionLogPath(session)) + "/" + port;
        Files.createDirectories(Paths.get(logPath));
        String configPath = getProcessWorkingPath(SystemSessionProperties.getNativeExecutionConfigPath(session)) + "/" + port;
        Files.createDirectories(Paths.get(configPath));

        populateConfigurationFiles(configPath);
        ProcessBuilder processBuilder = new ProcessBuilder(executablePath, "--log_dir", logPath, "--v", "1", "--etc_dir", configPath);
        processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        processBuilder.redirectError(ProcessBuilder.Redirect.INHERIT);
        try {
            log.info("Launching %s \nConfig path: %s\nLogging path: %s", executablePath, configPath, logPath);
            process = processBuilder.start();
        }
        catch (IOException e) {
            throw new RuntimeException(format("Cannot start %s", processBuilder.command()), e);
        }

        workerClient.getServerInfoWithRetry().get();
        // workerClient.getServerInfo().get();
    }

    public NativeExecutionTask createTask(
            TaskId taskId,
            PlanFragment fragment,
            List<TaskSource> sources,
            TableWriteInfo tableWriteInfo)
    {
        if (nativeExecutionTask == null) {
            nativeExecutionTask = new NativeExecutionTask(
                    session,
                    getTaskUri(location),
                    taskId,
                    fragment,
                    sources,
                    httpClient,
                    tableWriteInfo,
                    new Duration(200, TimeUnit.SECONDS),
                    executor,
                    errorRetryScheduledExecutor,
                    updateScheduledExecutor,
                    taskInfoCodec,
                    planFragmentCodec,
                    taskUpdateRequestCodec,
                    serverInfoCodec,
                    taskManagerConfig);
        }
        return nativeExecutionTask;
    }
}
