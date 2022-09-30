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
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.NATIVE_EXECUTION_CONFIG_PATH;
import static com.facebook.presto.SystemSessionProperties.NATIVE_EXECUTION_ENABLED;
import static com.facebook.presto.SystemSessionProperties.NATIVE_EXECUTION_EXECUTABLE_PATH;
import static com.facebook.presto.SystemSessionProperties.NATIVE_EXECUTION_LOG_PATH;
import static com.facebook.presto.hive.HiveSessionProperties.PUSHDOWN_FILTER_ENABLED;

public class TestPrestoSparkNativeExecution
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return PrestoSparkQueryRunner.createHivePrestoSparkQueryRunner();
    }

    @Test
    public void testNativeExecutionWithTableWrite()
    {
        assertUpdate("CREATE TABLE test_order WITH (format = 'DWRF') as SELECT orderkey, custkey FROM orders", 15000);
        // assertUpdate("CREATE TABLE test_order WITH (format = 'DWRF') as SELECT orderkey, custkey FROM orders LIMIT 10", 10);
        Session session = Session.builder(getSession())
                .setSystemProperty(NATIVE_EXECUTION_ENABLED, "true")
                .setSystemProperty(NATIVE_EXECUTION_EXECUTABLE_PATH, "/Users/mjdeng/Projects/presto/presto-native-execution/cmake-build-debug/presto_cpp/main/presto_server")
                .setSystemProperty(NATIVE_EXECUTION_LOG_PATH, "/Users/mjdeng/logs/")
                .setSystemProperty(NATIVE_EXECUTION_CONFIG_PATH, "/tmp")
                .setSystemProperty("table_writer_merge_operator_enabled", "false")
                .setCatalogSessionProperty("hive", "collect_column_statistics_on_write", "false")
                .setCatalogSessionProperty("hive", PUSHDOWN_FILTER_ENABLED, "true")
                .build();
        assertUpdate(session, "CREATE TABLE test_tablescan WITH (format = 'DWRF') as SELECT orderkey, custkey FROM test_order", 15000);
        // assertUpdate(session, "CREATE TABLE test_tablescan WITH (format = 'DWRF') as SELECT orderkey, custkey FROM test_order", 10);
        MaterializedResult value = computeActual(session, "SELECT * FROM test_tablescan");
        return;
    }

//    @Test
//    public void testNativeExecutionWithTableWrite2()
//    {
//        // assertUpdate("CREATE TABLE test_order as SELECT orderkey, custkey FROM orders LIMIT 10", 10);
////        Session session = Session.builder(getSession())
////                .setSystemProperty(NATIVE_EXECUTION_ENABLED, "true")
////                .setSystemProperty(NATIVE_EXECUTION_EXECUTABLE_PATH, "/Users/mjdeng/Projects/presto/presto-native-execution/cmake-build-debug/presto_cpp/main/presto_server")
////                .setSystemProperty("table_writer_merge_operator_enabled", "false")
////                .setCatalogSessionProperty("hive", "collect_column_statistics_on_write", "false")
////                .build();
//
//        Session session2 = Session.builder(getSession()).build();
////                .setSystemProperty("table_writer_merge_operator_enabled", "false")
////                .setCatalogSessionProperty("hive", "collect_column_statistics_on_write", "false")
////                .build();
//        Session session = session2;
//
//        // Expecting 0 row updated since currently the NativeExecutionOperator is dummy.
//        // assertUpdate(session, "CREATE TABLE test_tablescan as SELECT orderkey, custkey FROM test_order", 0);
//        SubPlan plan1 = subplan("CREATE TABLE test_order1 as SELECT orderkey, custkey FROM orders", session);
//        SubPlan plan2 = subplan("CREATE TABLE test_order2 as SELECT orderkey, custkey FROM orders", session2);
//
//        JsonCodec<PlanFragment> codec = jsonCodec(PlanFragment.class);
//        codec.toJson(plan2.getChildren().get(0).getFragment());
//        plan2.getChildren().get(0).getFragment().toBytes(codec);
//        plan1.getChildren().get(0).getFragment().toBytes(codec);
//
//        Assert.assertEquals(plan1, plan2);
//    }
}
