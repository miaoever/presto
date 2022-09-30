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
package com.facebook.presto.operator.common;

import com.google.common.collect.ImmutableMap;

public class WorkerConfigProperties
        extends Properties
{
    public static final String CONCURRENT_LIFESPANS_PER_TASK = "concurrent-lifespans-per-task";
    public static final String ENABLE_SERIALIZED_PAGE_CHECKSUM = "enable-serialized-page-checksum";
    public static final String ENABLE_VELOX_EXPRESSION_LOGGING = "enable_velox_expression_logging";
    public static final String ENABLE_VELOX_TASK_LOGGING = "enable_velox_task_logging";
    public static final String HTTP_SERVER_HTTP_PORT = "http-server.http.port";
    public static final String HTTP_EXEC_THREADS = "http_exec_threads";
    public static final String NUM_IO_THREADS = "num-io-threads";
    public static final String PRESTO_VERSION = "presto.version";
    public static final String SHUTDOWN_ONSET_SEC = "shutdown-onset-sec";
    public static final String SYSTEM_MEMORY_GB = "system-memory-gb";
    public static final String TASK_MAX_DRIVERS_PER_TASK = "task.max-drivers-per-task";
    public static final String DISCOVERY_URI = "discovery.uri";

    private static final String DEFAULT_CONCURRENT_LIFESPANS_PER_TASK = "5";
    private static final String DEFAULT_ENABLE_SERIALIZED_PAGE_CHECKSUM = "true";
    private static final String DEFAULT_ENABLE_VELOX_EXPRESSION_LOGGING = "false";
    private static final String DEFAULT_ENABLE_VELOX_TASK_LOGGING = "true";
    private static final String DEFAULT_HTTP_SERVER_HTTP_PORT = "7777";
    private static final String DEFAULT_HTTP_EXEC_THREADS = "128";
    private static final String DEFAULT_NUM_IO_THREADS = "30";
    private static final String DEFAULT_PRESTO_VERSION = "0.277.fb-1.cpp-3";
    private static final String DEFAULT_SHUTDOWN_ONSET_SEC = "10";
    private static final String DEFAULT_SYSTEM_MEMORY_GB = "10";
    private static final String DEFAULT_TASK_MAX_DRIVERS_PER_TASK = "15";
    private static final String DEFAULT_DISCOVERY_URI = "http://127.0.0.1";

    private WorkerConfigProperties(PropertiesPopulator populator, ImmutableMap<String, String> propertiesMap)
    {
        super(populator, propertiesMap);
    }

    public static class WorkerConfigPropertiesBuilder
            extends Properties.PropertiesBuilder<WorkerConfigPropertiesBuilder>
    {
        public WorkerConfigPropertiesBuilder initializeDefault()
        {
            propertiesMap.put(CONCURRENT_LIFESPANS_PER_TASK, DEFAULT_CONCURRENT_LIFESPANS_PER_TASK);
            propertiesMap.put(ENABLE_SERIALIZED_PAGE_CHECKSUM, DEFAULT_ENABLE_SERIALIZED_PAGE_CHECKSUM);
            propertiesMap.put(ENABLE_VELOX_EXPRESSION_LOGGING, DEFAULT_ENABLE_VELOX_EXPRESSION_LOGGING);
            propertiesMap.put(ENABLE_VELOX_TASK_LOGGING, DEFAULT_ENABLE_VELOX_TASK_LOGGING);
            propertiesMap.put(HTTP_SERVER_HTTP_PORT, DEFAULT_HTTP_SERVER_HTTP_PORT);
            propertiesMap.put(HTTP_EXEC_THREADS, DEFAULT_HTTP_EXEC_THREADS);
            propertiesMap.put(NUM_IO_THREADS, DEFAULT_NUM_IO_THREADS);
            propertiesMap.put(PRESTO_VERSION, DEFAULT_PRESTO_VERSION);
            propertiesMap.put(SHUTDOWN_ONSET_SEC, DEFAULT_SHUTDOWN_ONSET_SEC);
            propertiesMap.put(SYSTEM_MEMORY_GB, DEFAULT_SYSTEM_MEMORY_GB);
            propertiesMap.put(TASK_MAX_DRIVERS_PER_TASK, DEFAULT_TASK_MAX_DRIVERS_PER_TASK);
            propertiesMap.put(DISCOVERY_URI, DEFAULT_DISCOVERY_URI);
            return this;
        }

        @Override
        public WorkerConfigPropertiesBuilder getThis()
        {
            return this;
        }

        // TODO: Create setter for each property
    }
}
