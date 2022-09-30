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
package com.facebook.presto.spark.planner;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.Session;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.execution.ExplainAnalyzeContext;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.index.IndexManager;
import com.facebook.presto.memory.MemoryManagerConfig;
import com.facebook.presto.metadata.ConnectorMetadataUpdaterManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.FragmentResultCacheManager;
import com.facebook.presto.operator.LookupJoinOperators;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.OutputFactory;
import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.operator.TableCommitContext;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.operator.index.IndexJoinLookupStats;
import com.facebook.presto.spark.execution.NativeExecutionOperator;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spiller.PartitioningSpillerFactory;
import com.facebook.presto.spiller.SingleStreamSpillerFactory;
import com.facebook.presto.spiller.SpillerFactory;
import com.facebook.presto.spiller.StandaloneSpillerFactory;
import com.facebook.presto.split.PageSinkManager;
import com.facebook.presto.split.PageSourceProvider;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.facebook.presto.sql.gen.JoinFilterFunctionCompiler;
import com.facebook.presto.sql.gen.OrderingCompiler;
import com.facebook.presto.sql.gen.PageFunctionCompiler;
import com.facebook.presto.sql.planner.LocalExecutionPlanner;
import com.facebook.presto.sql.planner.NodePartitioningManager;
import com.facebook.presto.sql.planner.OutputPartitioning;
import com.facebook.presto.sql.planner.PartitioningProviderManager;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.RemoteSourceFactory;
import com.facebook.presto.sql.planner.plan.NativeExecutionNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;

import javax.inject.Inject;

import java.util.Optional;

import static com.facebook.presto.operator.PipelineExecutionStrategy.UNGROUPED_EXECUTION;
import static java.util.Objects.requireNonNull;

public class PrestoSparkLocalExecutionPlanner
        extends LocalExecutionPlanner
{
    private final JsonCodec<TaskSource> taskSourceCodec;
    private final JsonCodec<PlanFragment> planFragmentCodec;
    private final JsonCodec<TableWriteInfo> tableWriteInfoCodec;

    @Inject
    public PrestoSparkLocalExecutionPlanner(
            Metadata metadata,
            Optional<ExplainAnalyzeContext> explainAnalyzeContext,
            PageSourceProvider pageSourceProvider,
            IndexManager indexManager,
            PartitioningProviderManager partitioningProviderManager,
            NodePartitioningManager nodePartitioningManager,
            PageSinkManager pageSinkManager,
            ConnectorMetadataUpdaterManager metadataUpdaterManager,
            ExpressionCompiler expressionCompiler,
            PageFunctionCompiler pageFunctionCompiler,
            JoinFilterFunctionCompiler joinFilterFunctionCompiler,
            IndexJoinLookupStats indexJoinLookupStats,
            TaskManagerConfig taskManagerConfig,
            MemoryManagerConfig memoryManagerConfig,
            SpillerFactory spillerFactory,
            SingleStreamSpillerFactory singleStreamSpillerFactory,
            PartitioningSpillerFactory partitioningSpillerFactory,
            BlockEncodingSerde blockEncodingSerde,
            PagesIndex.Factory pagesIndexFactory,
            JoinCompiler joinCompiler,
            LookupJoinOperators lookupJoinOperators,
            OrderingCompiler orderingCompiler,
            JsonCodec<TableCommitContext> tableCommitContextCodec,
            DeterminismEvaluator determinismEvaluator,
            FragmentResultCacheManager fragmentResultCacheManager,
            ObjectMapper objectMapper,
            StandaloneSpillerFactory standaloneSpillerFactory,
            JsonCodec<TaskSource> taskSourceCodec,
            JsonCodec<TableWriteInfo> tableWriteInfoCodec,
            JsonCodec<PlanFragment> planFragmentCodec)
    {
        super(metadata,
                explainAnalyzeContext,
                pageSourceProvider, indexManager,
                partitioningProviderManager,
                nodePartitioningManager,
                pageSinkManager,
                metadataUpdaterManager,
                expressionCompiler,
                pageFunctionCompiler,
                joinFilterFunctionCompiler,
                indexJoinLookupStats,
                taskManagerConfig,
                memoryManagerConfig,
                spillerFactory,
                singleStreamSpillerFactory,
                partitioningSpillerFactory,
                blockEncodingSerde,
                pagesIndexFactory,
                joinCompiler,
                lookupJoinOperators,
                orderingCompiler,
                tableCommitContextCodec,
                determinismEvaluator,
                fragmentResultCacheManager,
                objectMapper,
                standaloneSpillerFactory);

        this.taskSourceCodec = requireNonNull(taskSourceCodec, "taskSourceCodec is null");
        this.tableWriteInfoCodec = requireNonNull(tableWriteInfoCodec, "tableWriteInfoCodec is null");
        this.planFragmentCodec = requireNonNull(planFragmentCodec, "planFragmentCodec is null");
    }

    public LocalExecutionPlan plan(
            TaskContext taskContext,
            PlanFragment planFragment,
            OutputFactory outputFactory,
            RemoteSourceFactory remoteSourceFactory,
            TableWriteInfo tableWriteInfo,
            boolean pageSinkCommitRequired)
    {
        return plan(
                taskContext,
                planFragment,
                outputFactory,
                createOutputPartitioning(taskContext, planFragment.getPartitioningScheme()),
                remoteSourceFactory,
                tableWriteInfo,
                pageSinkCommitRequired);
    }

    @VisibleForTesting
    public LocalExecutionPlan plan(
            TaskContext taskContext,
            PlanFragment planFragment,
            OutputFactory outputOperatorFactory,
            Optional<OutputPartitioning> outputPartitioning,
            RemoteSourceFactory remoteSourceFactory,
            TableWriteInfo tableWriteInfo,
            boolean pageSinkCommitRequired)
    {
        Session session = taskContext.getSession();
        PlanNode plan = planFragment.getRoot();
        LocalExecutionPlanContext context = new LocalExecutionPlanContext(taskContext, tableWriteInfo);
        PhysicalOperation physicalOperation = plan.accept(
                new PrestoSparkVisitor(session, planFragment, remoteSourceFactory, pageSinkCommitRequired, taskSourceCodec, tableWriteInfoCodec, planFragmentCodec), context);

        addDriverFactory(context, physicalOperation, outputOperatorFactory, planFragment.getPartitioningScheme(), outputPartitioning, plan, session);
        addLookupOuterDrivers(context);

        // notify operator factories that planning has completed
        notifyPlanningCompletion(context);

        return createLocalExecutionPlan(context, planFragment.getTableScanSchedulingOrder(), planFragment.getStageExecutionDescriptor());
    }

    private class PrestoSparkVisitor
            extends Visitor
    {
        private final PlanFragment planFragment;
        private final JsonCodec<TaskSource> taskSourceCodec;
        private final JsonCodec<PlanFragment> planFragmentCodec;
        private final JsonCodec<TableWriteInfo> tableWriteInfoCodec;

        private PrestoSparkVisitor(
                Session session,
                PlanFragment planFragment,
                RemoteSourceFactory remoteSourceFactory,
                boolean pageSinkCommitRequired,
                JsonCodec<TaskSource> taskSourceCodec,
                JsonCodec<TableWriteInfo> tableWriteInfoCodec,
                JsonCodec<PlanFragment> planFragmentCodec)
        {
            super(session, planFragment.getStageExecutionDescriptor(), remoteSourceFactory, pageSinkCommitRequired);
            this.taskSourceCodec = requireNonNull(taskSourceCodec, "taskSourceCodec is null");
            this.tableWriteInfoCodec = requireNonNull(tableWriteInfoCodec, "tableWriteInfoCodec is null");
            this.planFragmentCodec = requireNonNull(planFragmentCodec, "planFragmentCodec is null");
            this.planFragment = requireNonNull(planFragment, "planFragment is null");
        }

        @Override
        public PhysicalOperation visitNativeExecution(NativeExecutionNode node, LocalExecutionPlanContext context)
        {
            OperatorFactory operatorFactory = new NativeExecutionOperator.NativeExecutionOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    taskSourceCodec,
                    planFragmentCodec,
                    tableWriteInfoCodec,
                    planFragment.withSubPlan(node.getSubPlan()),
                    context.getTableWriteInfo());
            return new PhysicalOperation(operatorFactory, makeLayout(node), context, UNGROUPED_EXECUTION);
        }
    }
}
