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
package com.facebook.presto.spark;

import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.SystemSessionProperties.NATIVE_EXECUTION_ENABLED;

public class TestPrestoSparkNativeExecution
        extends AbstractTestQueryFramework
{
    private static final String SPARK_SHUFFLE_MANAGER = "spark.shuffle.manager";
    private static final String DEFAULT_SPARK_SHUFFLE_MANAGER = "spark.default.shuffle.manager";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return PrestoSparkQueryRunner.createHivePrestoSparkQueryRunner();
    }

    @Test
    public void testNativeExecutionWithTableWrite()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(NATIVE_EXECUTION_ENABLED, "true")
                .setSystemProperty("table_writer_merge_operator_enabled", "false")
                .setCatalogSessionProperty("hive", "collect_column_statistics_on_write", "false")
                .build();

        // Expecting 0 row updated since currently the NativeExecutionOperator is dummy.
        assertUpdate(session, "CREATE TABLE test_tablescan as SELECT orderkey, custkey FROM orders", 0);
    }

    public static Map<String, String> getNativeExecutionShuffleConfigs()
    {
        Map<String, String> SparkConfigs = new HashMap<>();
        SparkConfigs.put(SPARK_SHUFFLE_MANAGER, "com.facebook.presto.spark.classloader_interface.PrestoSparkNativeExecutionShuffleManager");
        SparkConfigs.put(DEFAULT_SPARK_SHUFFLE_MANAGER, "org.apache.spark.shuffle.sort.SortShuffleManager");
        return SparkConfigs;
    }

    @Test
    public void testNativeExecutionWithShuffle()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(NATIVE_EXECUTION_ENABLED, "true")
                .setSystemProperty("table_writer_merge_operator_enabled", "false")
                .setCatalogSessionProperty("hive", "collect_column_statistics_on_write", "false")
                .build();

        PrestoSparkQueryRunner queryRunner = (PrestoSparkQueryRunner) getQueryRunner();
        queryRunner.resetSparkContext(getNativeExecutionShuffleConfigs());
        // Expecting 0 row updated since currently the NativeExecutionOperator is dummy.
        queryRunner.execute(session, "CREATE TABLE test_aggregate as SELECT  partkey, count(*) c FROM lineitem WHERE partkey % 10 = 1 GROUP BY partkey");
        //assertUpdate(session, "CREATE TABLE test_aggregate as SELECT  partkey, count(*) c FROM lineitem WHERE partkey % 10 = 1 GROUP BY partkey", 0);
    }
}
