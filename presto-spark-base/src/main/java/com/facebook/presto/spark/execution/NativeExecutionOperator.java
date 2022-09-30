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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.Page;
import com.facebook.presto.execution.ScheduledSplit;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.SourceOperator;
import com.facebook.presto.operator.SourceOperatorFactory;
import com.facebook.presto.operator.SplitOperatorInfo;
import com.facebook.presto.operator.WrapperSourceOperator;
import com.facebook.presto.spi.UpdatablePageSource;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.sql.planner.PlanFragment;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class NativeExecutionOperator
        implements SourceOperator, WrapperSourceOperator
{
    private final PlanNodeId sourceId;
    private final OperatorContext operatorContext;
    private final LocalMemoryContext systemMemoryContext;
    private final JsonCodec<TaskSource> taskSourceCodec;
    private final JsonCodec<PlanFragment> planFragmentCodec;
    private final JsonCodec<TableWriteInfo> tableWriteInfoCodec;
    private final PlanFragment planFragment;
    private final TableWriteInfo tableWriteInfo;
    private TaskSource taskSource;
    private final boolean isFirstOperator;

    private boolean finished;

    public NativeExecutionOperator(
            PlanNodeId sourceId,
            OperatorContext operatorContext,
            JsonCodec<TaskSource> taskSourceCodec,
            JsonCodec<PlanFragment> planFragmentCodec,
            JsonCodec<TableWriteInfo> tableWriteInfoCodec,
            PlanFragment planFragment,
            TableWriteInfo tableWriteInfo,
            boolean isFirstOperator)
    {
        this.sourceId = requireNonNull(sourceId, "sourceId is null");
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.systemMemoryContext = operatorContext.localSystemMemoryContext();
        this.taskSourceCodec = requireNonNull(taskSourceCodec, "taskSourceJsonCodec is null");
        this.planFragmentCodec = requireNonNull(planFragmentCodec, "planFragmentCodec is null");
        this.tableWriteInfoCodec = requireNonNull(tableWriteInfoCodec, "tableWriteInfoCodec is null");
        this.planFragment = requireNonNull(planFragment, "planFragment is null");
        this.tableWriteInfo = requireNonNull(tableWriteInfo, "tableWriteInfo is null");
        this.taskSource = null;
        this.finished = false;
        this.isFirstOperator = isFirstOperator;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public boolean needsInput()
    {
        return false;
    }

    @Override
    public void addInput(Page page)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Page getOutput()
    {
        if (finished) {
            return null;
        }
        finished = true;
        return null;
    }

    @Override
    public void finish()
    {
        finished = true;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public PlanNodeId getSourceId()
    {
        return sourceId;
    }

    @Override
    public Supplier<Optional<UpdatablePageSource>> addSplit(ScheduledSplit split)
    {
        requireNonNull(split, "split is null");
        checkState(this.taskSource == null, "NativeEngine operator split already set");

        if (finished) {
            return Optional::empty;
        }

        this.taskSource = new TaskSource(split.getPlanNodeId(), ImmutableSet.of(split), true);

        Object splitInfo = split.getSplit().getInfo();
        Map<String, String> infoMap = split.getSplit().getInfoMap();

        //Make the implicit assumption that if infoMap is populated we can use that instead of the raw object.
        if (infoMap != null && !infoMap.isEmpty()) {
            operatorContext.setInfoSupplier(Suppliers.ofInstance(new SplitOperatorInfo(infoMap)));
        }
        else if (splitInfo != null) {
            operatorContext.setInfoSupplier(Suppliers.ofInstance(new SplitOperatorInfo(splitInfo)));
        }

        return Optional::empty;
    }

    @Override
    public Supplier<Optional<UpdatablePageSource>> addSplit(Split split)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void noMoreSplits()
    {
        if (taskSource == null) {
            finished = true;
        }
    }

    @Override
    public void close()
    {
        systemMemoryContext.setBytes(0);
    }

    private List<TableScanNode> findTableScanNodes(PlanNode node)
    {
        return searchFrom(node)
                .where(TableScanNode.class::isInstance)
                .findAll();
    }

    public static class NativeExecutionOperatorFactory
            implements SourceOperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final JsonCodec<TaskSource> taskSourceCodec;
        private final JsonCodec<PlanFragment> planFragmentCodec;
        private final JsonCodec<TableWriteInfo> tableWriteInfoCodec;
        private final PlanFragment planFragment;
        private final TableWriteInfo tableWriteInfo;
        private boolean isFirstOperator = true;
        private boolean closed;

        public NativeExecutionOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                JsonCodec<TaskSource> taskSourceCodec,
                JsonCodec<PlanFragment> planFragmentCodec,
                JsonCodec<TableWriteInfo> tableWriteInfoCodec,
                PlanFragment planFragment,
                TableWriteInfo tableWriteInfo)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.taskSourceCodec = requireNonNull(taskSourceCodec, "taskSourceJsonCodec is null");
            this.planFragmentCodec = requireNonNull(planFragmentCodec, "planFragmentCodec is null");
            this.tableWriteInfoCodec = requireNonNull(tableWriteInfoCodec, "tableWriteInfoCodec is null");
            this.planFragment = requireNonNull(planFragment, "planFragment is null");
            this.tableWriteInfo = requireNonNull(tableWriteInfo, "tableWriteInfo is null");
        }

        @Override
        public PlanNodeId getSourceId()
        {
            return planNodeId;
        }

        @Override
        public SourceOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "operator factory is closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, NativeExecutionOperator.class.getSimpleName());
            SourceOperator operator = new NativeExecutionOperator(
                    planNodeId,
                    operatorContext,
                    taskSourceCodec,
                    planFragmentCodec,
                    tableWriteInfoCodec,
                    planFragment,
                    tableWriteInfo,
                    isFirstOperator);
            isFirstOperator = false;
            return operator;
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        public PlanFragment getPlanFragment()
        {
            return planFragment;
        }
    }
}
