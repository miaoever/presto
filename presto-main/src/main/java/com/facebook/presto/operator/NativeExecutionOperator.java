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

import com.facebook.presto.common.Page;
import com.facebook.presto.execution.ScheduledSplit;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.buffer.PagesSerdeFactory;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.operator.nativeexecution.NativeExecutionProcessFactory;
import com.facebook.presto.operator.nativeexecution.NativeExecutionTask;
import com.facebook.presto.spi.UpdatablePageSource;
import com.facebook.presto.spi.page.PagesSerde;
import com.facebook.presto.spi.page.SerializedPage;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.sql.planner.PlanFragment;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/*
 * TODO: The lifecycle of NativeExecutionOperator is:
 *  1. Launch the native engine external process when initialing the operator.
 *  2. Serialize and pass the planFragment, tableWriteInfo and taskSource to the external process through NativeExecutionTask APIs.
 *  3. Block on NativeExecutionTask's pollResult call to retrieve the data in {@link SerializedPage} page format back external process.
 *  4. Deserialize the serialized page to presto Page and return it back to driver from the getOutput method.
 *  5. The close() will be called by the driver when the pollResult returns empty and the Future of NativeExecutionTask has completed.
 *  6. Shut down the external process upon calling of close() method
 */
public class NativeExecutionOperator
        implements SourceOperator
{
    private final PlanNodeId sourceId;
    private final OperatorContext operatorContext;
    private final LocalMemoryContext systemMemoryContext;
    private final PlanFragment planFragment;
    private final TableWriteInfo tableWriteInfo;
    private final PagesSerde serde;
    private final NativeExecutionProcess process;
    private final NativeExecutionProcessFactory taskFactory;
    private static final String NATIVE_EXECUTION_TASK_URI = "http://127.0.0.1:7777/v1/task/";
    private static final String NATIVE_EXECUTION_SERVER_URI = "http://127.0.0.1:7777/v1/info";
    private NativeExecutionTask task;
    private CompletableFuture<Void> taskStatusFuture;
    private TaskSource taskSource;
    private boolean finished;

    public NativeExecutionOperator(
            PlanNodeId sourceId,
            OperatorContext operatorContext,
            PlanFragment planFragment,
            TableWriteInfo tableWriteInfo,
            PagesSerde serde,
            NativeExecutionProcess process,
            NativeExecutionProcessFactory taskFactory)
    {
        this.sourceId = requireNonNull(sourceId, "sourceId is null");
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.systemMemoryContext = operatorContext.localSystemMemoryContext();
        this.planFragment = requireNonNull(planFragment, "planFragment is null");
        this.tableWriteInfo = requireNonNull(tableWriteInfo, "tableWriteInfo is null");
        this.process = requireNonNull(process, "process is null");
        this.taskFactory = requireNonNull(taskFactory, "taskFactory is null");
        this.taskSource = null;
        this.finished = false;
        this.serde = serde;

        try {
            this.process.start();
        }
        catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
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

    /*
     * TODO: The getOutput method will call NativeExecutionTask APIs to:
     *  1. Submit the plan to the external process
     *  2. Call pollResult method to get latest buffered result.
     *  3. Call getTaskInfo method to get the TaskInfo and propagate it
     *  4. Deserialize the {@link SerializedPage} page to presto Page and return it back
     */
    @Override
    public Page getOutput()
    {
        if (finished) {
            return null;
        }

        if (task == null) {
            checkState(taskSource != null, "taskSource is null");
            checkState(taskStatusFuture == null, "taskStatusFuture has already been set");

            this.task = taskFactory.createNativeExecutionTask(
                    operatorContext.getSession(),
                    URI.create(NATIVE_EXECUTION_TASK_URI),
                    operatorContext.getDriverContext().getTaskId(),
                    planFragment,
                    ImmutableList.of(taskSource),
                    tableWriteInfo);
            taskStatusFuture = task.start();
        }

        try {
            if (taskStatusFuture.isDone()) {
                Optional<SerializedPage> page = task.pollResult();
                if (page.isPresent()) {
                    return processResult(page.get());
                }
                else {
                    finished = true;
                    return null;
                }
            }

            Optional<SerializedPage> page = task.pollResult();
            if (page.isPresent()) {
                return processResult(page.get());
            }
            else {
                return null;
            }
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private Page processResult(SerializedPage page)
    {
        operatorContext.recordRawInput(page.getSizeInBytes(), page.getPositionCount());
        Page deserializedPage = serde.deserialize(page);
        operatorContext.recordProcessedInput(deserializedPage.getSizeInBytes(), page.getPositionCount());
        return deserializedPage;
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
    public void noMoreSplits()
    {
    }

    @Override
    public void close()
    {
        systemMemoryContext.setBytes(0);
        task.stop();
        process.close();
    }

    public static class NativeExecutionOperatorFactory
            implements SourceOperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final PlanFragment planFragment;
        private final TableWriteInfo tableWriteInfo;
        private final PagesSerdeFactory serdeFactory;
        private final NativeExecutionProcessFactory taskFactory;
        private boolean closed;

        public NativeExecutionOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                PlanFragment planFragment,
                TableWriteInfo tableWriteInfo,
                PagesSerdeFactory serdeFactory,
                NativeExecutionProcessFactory taskFactory)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.planFragment = requireNonNull(planFragment, "planFragment is null");
            this.tableWriteInfo = requireNonNull(tableWriteInfo, "tableWriteInfo is null");
            this.serdeFactory = requireNonNull(serdeFactory, "serdeFactory is null");
            this.taskFactory = requireNonNull(taskFactory, "taskFactory is null");
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
                    planFragment,
                    tableWriteInfo,
                    serdeFactory.createPagesSerde(),
                    taskFactory.createNativeExecutionProcess(operatorContext.getSession(), URI.create(NATIVE_EXECUTION_SERVER_URI)),
                    taskFactory);
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
