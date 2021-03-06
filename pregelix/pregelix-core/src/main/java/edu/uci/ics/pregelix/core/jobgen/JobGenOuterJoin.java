/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.pregelix.core.jobgen;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.external.connector.asterixdb.data.TypeTraits;
import org.apache.hyracks.api.constraints.PartitionConstraintHelper;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.INullWriterFactory;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.connectors.MToNPartitioningConnectorDescriptor;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.storage.am.common.dataflow.TreeIndexInsertUpdateDeleteOperatorDescriptor;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import edu.uci.ics.pregelix.api.graph.MsgList;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.pregelix.core.optimizer.IOptimizer;
import edu.uci.ics.pregelix.core.util.DataflowUtils;
import edu.uci.ics.pregelix.dataflow.ConnectorPolicyAssignmentPolicy;
import edu.uci.ics.pregelix.dataflow.EmptySinkOperatorDescriptor;
import edu.uci.ics.pregelix.dataflow.EmptyTupleSourceOperatorDescriptor;
import edu.uci.ics.pregelix.dataflow.FinalAggregateOperatorDescriptor;
import edu.uci.ics.pregelix.dataflow.MaterializingReadOperatorDescriptor;
import edu.uci.ics.pregelix.dataflow.MaterializingWriteOperatorDescriptor;
import edu.uci.ics.pregelix.dataflow.TerminationStateWriterOperatorDescriptor;
import edu.uci.ics.pregelix.dataflow.base.IConfigurationFactory;
import edu.uci.ics.pregelix.dataflow.std.IndexNestedLoopJoinFunctionUpdateOperatorDescriptor;
import edu.uci.ics.pregelix.dataflow.std.RuntimeHookOperatorDescriptor;
import edu.uci.ics.pregelix.dataflow.std.TreeSearchFunctionUpdateOperatorDescriptor;
import edu.uci.ics.pregelix.dataflow.std.base.IRecordDescriptorFactory;
import edu.uci.ics.pregelix.dataflow.std.base.IRuntimeHookFactory;
import edu.uci.ics.pregelix.runtime.function.ComputeUpdateFunctionFactory;
import edu.uci.ics.pregelix.runtime.function.StartComputeUpdateFunctionFactory;
import edu.uci.ics.pregelix.runtime.touchpoint.MergePartitionComputerFactory;
import edu.uci.ics.pregelix.runtime.touchpoint.MsgListNullWriterFactory;
import edu.uci.ics.pregelix.runtime.touchpoint.PostSuperStepRuntimeHookFactory;
import edu.uci.ics.pregelix.runtime.touchpoint.PreSuperStepRuntimeHookFactory;
import edu.uci.ics.pregelix.runtime.touchpoint.RuntimeHookFactory;
import edu.uci.ics.pregelix.runtime.touchpoint.VertexIdNullWriterFactory;

public class JobGenOuterJoin extends JobGen {

    public JobGenOuterJoin(PregelixJob job, IOptimizer optimizer) {
        super(job, optimizer);
    }

    public JobGenOuterJoin(PregelixJob job, String jobId, IOptimizer optimizer) {
        super(job, jobId, optimizer);
    }

    @Override
    protected JobSpecification generateFirstIteration(int iteration) throws HyracksException {
        Class<? extends WritableComparable<?>> vertexIdClass = BspUtils.getVertexIndexClass(conf);
        Class<? extends Writable> vertexClass = BspUtils.getVertexClass(conf);
        Class<? extends Writable> messageValueClass = BspUtils.getMessageValueClass(conf);
        String[] partialAggregateValueClassNames = BspUtils.getPartialAggregateValueClassNames(conf);

        IConfigurationFactory confFactory = getConfigurationFactory();
        JobSpecification spec = new JobSpecification(frameSize);
	
        /**
         * construct empty tuple operator
         */
        EmptyTupleSourceOperatorDescriptor emptyTupleSource = new EmptyTupleSourceOperatorDescriptor(spec);
        setLocationConstraint(spec, emptyTupleSource);

        /** construct runtime hook */
        RuntimeHookOperatorDescriptor preSuperStep = new RuntimeHookOperatorDescriptor(spec,
                new PreSuperStepRuntimeHookFactory(jobId, confFactory));
        setLocationConstraint(spec, preSuperStep);

        /**
         * construct btree search function update operator
         */
        RecordDescriptor recordDescriptor = DataflowUtils.getRecordDescriptorFromKeyValueClasses(conf,
                vertexIdClass.getName(), vertexClass.getName());
        IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[1];
        comparatorFactories[0] = JobGenUtil.getIBinaryComparatorFactory(iteration, vertexIdClass);;
        IFileSplitProvider fileSplitProvider = getFileSplitProvider(jobId, PRIMARY_INDEX);

        ITypeTraits[] typeTraits = new ITypeTraits[2];
        typeTraits[0] = new TypeTraits(false);
        typeTraits[1] = new TypeTraits(false);

        RecordDescriptor rdDummy = DataflowUtils.getRecordDescriptorFromWritableClasses(conf,
                VLongWritable.class.getName());
        RecordDescriptor rdPartialAggregate = DataflowUtils.getRecordDescriptorFromWritableClasses(conf,
                partialAggregateValueClassNames);
        IConfigurationFactory configurationFactory = getConfigurationFactory();
        IRuntimeHookFactory preHookFactory = new RuntimeHookFactory(configurationFactory);
        IRecordDescriptorFactory inputRdFactory = DataflowUtils.getWritableRecordDescriptorFactoryFromWritableClasses(
                getConfigurationFactory(), vertexIdClass.getName(), vertexClass.getName());
        RecordDescriptor rdUnnestedMessage = DataflowUtils.getRecordDescriptorFromKeyValueClasses(conf,
                vertexIdClass.getName(), messageValueClass.getName());
        RecordDescriptor rdInsert = DataflowUtils.getRecordDescriptorFromKeyValueClasses(conf, vertexIdClass.getName(),
                vertexClass.getName());
        RecordDescriptor rdDelete = DataflowUtils.getRecordDescriptorFromWritableClasses(conf, vertexIdClass.getName());
        RecordDescriptor rdFinal = DataflowUtils.getRecordDescriptorFromKeyValueClasses(conf, vertexIdClass.getName(),
                MsgList.class.getName());

        TreeSearchFunctionUpdateOperatorDescriptor scanner = new TreeSearchFunctionUpdateOperatorDescriptor(spec,
                recordDescriptor, storageManagerInterface, lcManagerProvider, fileSplitProvider, typeTraits,
                comparatorFactories, JobGenUtil.getForwardScan(iteration), null, null, true, true,
                getIndexDataflowHelperFactory(), inputRdFactory, 5, new StartComputeUpdateFunctionFactory(confFactory),
                preHookFactory, null, rdUnnestedMessage, rdDummy, rdPartialAggregate, rdInsert, rdDelete);
        setLocationConstraint(spec, scanner);

        /**
         * construct group-by operator pipeline
         */
        Pair<IOperatorDescriptor, IOperatorDescriptor> groupOps = generateGroupingOperators(spec, iteration,
                vertexIdClass);
        IOperatorDescriptor groupStartOperator = groupOps.getLeft();
        IOperatorDescriptor groupEndOperator = groupOps.getRight();

        /**
         * construct the materializing write operator
         */
        MaterializingWriteOperatorDescriptor materialize = new MaterializingWriteOperatorDescriptor(spec, rdFinal,
                jobId, iteration);
        setLocationConstraint(spec, materialize);

        RuntimeHookOperatorDescriptor postSuperStep = new RuntimeHookOperatorDescriptor(spec,
                new PostSuperStepRuntimeHookFactory(jobId));
        setLocationConstraint(spec, postSuperStep);

        /** construct empty sink operator */
        EmptySinkOperatorDescriptor emptySink2 = new EmptySinkOperatorDescriptor(spec);
        setLocationConstraint(spec, emptySink2);

        /**
         * termination state write operator
         */
        TerminationStateWriterOperatorDescriptor terminateWriter = new TerminationStateWriterOperatorDescriptor(spec,
                configurationFactory, jobId);
        PartitionConstraintHelper.addPartitionCountConstraint(spec, terminateWriter, 1);
        ITuplePartitionComputerFactory unifyingPartitionComputerFactory = new MergePartitionComputerFactory();

        /**
         * final aggregate write operator
         */
        IRecordDescriptorFactory aggRdFactory = DataflowUtils.getWritableRecordDescriptorFactoryFromWritableClasses(
                getConfigurationFactory(), partialAggregateValueClassNames);
        FinalAggregateOperatorDescriptor finalAggregator = new FinalAggregateOperatorDescriptor(spec,
                configurationFactory, aggRdFactory, jobId);
        PartitionConstraintHelper.addPartitionCountConstraint(spec, finalAggregator, 1);

        /**
         * add the insert operator to insert vertexes
         */
        int[] fieldPermutation = new int[] { 0, 1 };
        TreeIndexInsertUpdateDeleteOperatorDescriptor insertOp = new TreeIndexInsertUpdateDeleteOperatorDescriptor(
                spec, rdInsert, storageManagerInterface, lcManagerProvider, fileSplitProvider, typeTraits,
                comparatorFactories, null, fieldPermutation, IndexOperation.INSERT, getIndexDataflowHelperFactory(),
                null, NoOpOperationCallbackFactory.INSTANCE);
        setLocationConstraint(spec, insertOp);

        /**
         * add the delete operator to delete vertexes
         */
        int[] fieldPermutationDelete = new int[] { 0 };
        TreeIndexInsertUpdateDeleteOperatorDescriptor deleteOp = new TreeIndexInsertUpdateDeleteOperatorDescriptor(
                spec, rdDelete, storageManagerInterface, lcManagerProvider, fileSplitProvider, typeTraits,
                comparatorFactories, null, fieldPermutationDelete, IndexOperation.DELETE,
                getIndexDataflowHelperFactory(), null, NoOpOperationCallbackFactory.INSTANCE);
        setLocationConstraint(spec, deleteOp);

        /** construct empty sink operator */
        EmptySinkOperatorDescriptor emptySink3 = new EmptySinkOperatorDescriptor(spec);

        /** construct empty sink operator */
        EmptySinkOperatorDescriptor emptySink4 = new EmptySinkOperatorDescriptor(spec);
        setLocationConstraint(spec, emptySink4);

        ITuplePartitionComputerFactory partionFactory = getVertexPartitionComputerFactory();
        /** connect all operators **/
        spec.connect(new OneToOneConnectorDescriptor(spec), emptyTupleSource, 0, preSuperStep, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), preSuperStep, 0, scanner, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), scanner, 0, groupStartOperator, 0);
        spec.connect(new MToNPartitioningConnectorDescriptor(spec, unifyingPartitionComputerFactory), scanner, 1,
                terminateWriter, 0);
        spec.connect(new MToNPartitioningConnectorDescriptor(spec, unifyingPartitionComputerFactory), scanner, 2,
                finalAggregator, 0);

        /**
         * connect the insert/delete operator
         */
        spec.connect(new MToNPartitioningConnectorDescriptor(spec, partionFactory), scanner, 3, insertOp, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), insertOp, 0, emptySink3, 0);
        spec.connect(new MToNPartitioningConnectorDescriptor(spec, partionFactory), scanner, 4, deleteOp, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), deleteOp, 0, emptySink4, 0);

        /**
         * connect the group-by operator
         */
        spec.connect(new OneToOneConnectorDescriptor(spec), groupEndOperator, 0, materialize, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), materialize, 0, postSuperStep, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), postSuperStep, 0, emptySink2, 0);

        spec.addRoot(terminateWriter);
        spec.addRoot(finalAggregator);
        spec.addRoot(emptySink2);
        spec.addRoot(emptySink3);
        spec.addRoot(emptySink4);

        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy(spec));
        spec.setFrameSize(frameSize);
        return spec;
    }

    @Override
    protected JobSpecification generateNonFirstIteration(int iteration) throws HyracksException {
        Class<? extends WritableComparable<?>> vertexIdClass = BspUtils.getVertexIndexClass(conf);
        Class<? extends Writable> vertexClass = BspUtils.getVertexClass(conf);
        Class<? extends Writable> messageValueClass = BspUtils.getMessageValueClass(conf);
        String[] partialAggregateValueClassNames = BspUtils.getPartialAggregateValueClassNames(conf);
        JobSpecification spec = new JobSpecification(frameSize);

        /**
         * source aggregate
         */
        int[] keyFields = new int[] { 0 };
        RecordDescriptor rdUnnestedMessage = DataflowUtils.getRecordDescriptorFromKeyValueClasses(conf,
                vertexIdClass.getName(), messageValueClass.getName());
        IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[1];
        comparatorFactories[0] = JobGenUtil.getIBinaryComparatorFactory(iteration, vertexIdClass);
        RecordDescriptor rdFinal = DataflowUtils.getRecordDescriptorFromKeyValueClasses(conf, vertexIdClass.getName(),
                MsgList.class.getName());
        RecordDescriptor rdInsert = DataflowUtils.getRecordDescriptorFromKeyValueClasses(conf, vertexIdClass.getName(),
                vertexClass.getName());
        RecordDescriptor rdDelete = DataflowUtils.getRecordDescriptorFromWritableClasses(conf, vertexIdClass.getName());

        /**
         * construct empty tuple operator
         */
        EmptyTupleSourceOperatorDescriptor emptyTupleSource = new EmptyTupleSourceOperatorDescriptor(spec);
        setLocationConstraint(spec, emptyTupleSource);

        /**
         * construct pre-superstep hook
         */
        IConfigurationFactory confFactory = getConfigurationFactory();
        RuntimeHookOperatorDescriptor preSuperStep = new RuntimeHookOperatorDescriptor(spec,
                new PreSuperStepRuntimeHookFactory(jobId, confFactory));
        setLocationConstraint(spec, preSuperStep);

        /**
         * construct the materializing write operator
         */
        MaterializingReadOperatorDescriptor materializeRead = new MaterializingReadOperatorDescriptor(spec, rdFinal,
                true, jobId, iteration);
        setLocationConstraint(spec, materializeRead);

        /**
         * construct index join function update operator
         */
        IFileSplitProvider fileSplitProvider = getFileSplitProvider(jobId, PRIMARY_INDEX);
        ITypeTraits[] typeTraits = new ITypeTraits[2];
        typeTraits[0] = new TypeTraits(false);
        typeTraits[1] = new TypeTraits(false);
        INullWriterFactory[] nullWriterFactories = new INullWriterFactory[2];
        nullWriterFactories[0] = VertexIdNullWriterFactory.INSTANCE;
        nullWriterFactories[1] = MsgListNullWriterFactory.INSTANCE;

        RecordDescriptor rdDummy = DataflowUtils.getRecordDescriptorFromWritableClasses(conf,
                VLongWritable.class.getName());
        RecordDescriptor rdPartialAggregate = DataflowUtils.getRecordDescriptorFromWritableClasses(conf,
                partialAggregateValueClassNames);
        IConfigurationFactory configurationFactory = getConfigurationFactory();
        IRuntimeHookFactory preHookFactory = new RuntimeHookFactory(configurationFactory);
        IRecordDescriptorFactory inputRdFactory = DataflowUtils.getWritableRecordDescriptorFactoryFromWritableClasses(
                getConfigurationFactory(), vertexIdClass.getName(), MsgList.class.getName(), vertexIdClass.getName(),
                vertexClass.getName());

        IndexNestedLoopJoinFunctionUpdateOperatorDescriptor join = new IndexNestedLoopJoinFunctionUpdateOperatorDescriptor(
                spec, storageManagerInterface, lcManagerProvider, fileSplitProvider, typeTraits, comparatorFactories,
                JobGenUtil.getForwardScan(iteration), keyFields, keyFields, true, true,
                getIndexDataflowHelperFactory(), true, nullWriterFactories, inputRdFactory, 5,
                new ComputeUpdateFunctionFactory(confFactory), preHookFactory, null, rdUnnestedMessage, rdDummy,
                rdPartialAggregate, rdInsert, rdDelete);
        setLocationConstraint(spec, join);

        /**
         * construct group-by operator pipeline
         */
        Pair<IOperatorDescriptor, IOperatorDescriptor> groupOps = generateGroupingOperators(spec, iteration,
                vertexIdClass);
        IOperatorDescriptor groupStartOperator = groupOps.getLeft();
        IOperatorDescriptor groupEndOperator = groupOps.getRight();

        /**
         * construct the materializing write operator
         */
        MaterializingWriteOperatorDescriptor materialize = new MaterializingWriteOperatorDescriptor(spec, rdFinal,
                jobId, iteration);
        setLocationConstraint(spec, materialize);

        /** construct runtime hook */
        RuntimeHookOperatorDescriptor postSuperStep = new RuntimeHookOperatorDescriptor(spec,
                new PostSuperStepRuntimeHookFactory(jobId));
        setLocationConstraint(spec, postSuperStep);

        /** construct empty sink operator */
        EmptySinkOperatorDescriptor emptySink = new EmptySinkOperatorDescriptor(spec);
        setLocationConstraint(spec, emptySink);

        /**
         * termination state write operator
         */
        TerminationStateWriterOperatorDescriptor terminateWriter = new TerminationStateWriterOperatorDescriptor(spec,
                configurationFactory, jobId);
        PartitionConstraintHelper.addPartitionCountConstraint(spec, terminateWriter, 1);

        /**
         * final aggregate write operator
         */
        IRecordDescriptorFactory aggRdFactory = DataflowUtils.getWritableRecordDescriptorFactoryFromWritableClasses(
                getConfigurationFactory(), partialAggregateValueClassNames);
        FinalAggregateOperatorDescriptor finalAggregator = new FinalAggregateOperatorDescriptor(spec,
                configurationFactory, aggRdFactory, jobId);
        PartitionConstraintHelper.addPartitionCountConstraint(spec, finalAggregator, 1);

        /**
         * add the insert operator to insert vertexes
         */
        int[] fieldPermutation = new int[] { 0, 1 };
        TreeIndexInsertUpdateDeleteOperatorDescriptor insertOp = new TreeIndexInsertUpdateDeleteOperatorDescriptor(
                spec, rdInsert, storageManagerInterface, lcManagerProvider, fileSplitProvider, typeTraits,
                comparatorFactories, null, fieldPermutation, IndexOperation.INSERT, getIndexDataflowHelperFactory(),
                null, NoOpOperationCallbackFactory.INSTANCE);
        setLocationConstraint(spec, insertOp);

        /**
         * add the delete operator to delete vertexes
         */
        int[] fieldPermutationDelete = new int[] { 0 };
        TreeIndexInsertUpdateDeleteOperatorDescriptor deleteOp = new TreeIndexInsertUpdateDeleteOperatorDescriptor(
                spec, rdDelete, storageManagerInterface, lcManagerProvider, fileSplitProvider, typeTraits,
                comparatorFactories, null, fieldPermutationDelete, IndexOperation.DELETE,
                getIndexDataflowHelperFactory(), null, NoOpOperationCallbackFactory.INSTANCE);
        setLocationConstraint(spec, deleteOp);

        /** construct empty sink operator */
        EmptySinkOperatorDescriptor emptySink3 = new EmptySinkOperatorDescriptor(spec);
        setLocationConstraint(spec, emptySink3);

        /** construct empty sink operator */
        EmptySinkOperatorDescriptor emptySink4 = new EmptySinkOperatorDescriptor(spec);
        setLocationConstraint(spec, emptySink4);

        ITuplePartitionComputerFactory unifyingPartitionComputerFactory = new MergePartitionComputerFactory();
        ITuplePartitionComputerFactory partionFactory = getVertexPartitionComputerFactory();

        /** connect all operators **/
        spec.connect(new OneToOneConnectorDescriptor(spec), emptyTupleSource, 0, preSuperStep, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), preSuperStep, 0, materializeRead, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), materializeRead, 0, join, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), join, 0, groupStartOperator, 0);
        spec.connect(new MToNPartitioningConnectorDescriptor(spec, unifyingPartitionComputerFactory), join, 1,
                terminateWriter, 0);
        spec.connect(new MToNPartitioningConnectorDescriptor(spec, unifyingPartitionComputerFactory), join, 2,
                finalAggregator, 0);

        /**
         * connect the insert/delete operator
         */
        spec.connect(new MToNPartitioningConnectorDescriptor(spec, partionFactory), join, 3, insertOp, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), insertOp, 0, emptySink3, 0);
        spec.connect(new MToNPartitioningConnectorDescriptor(spec, partionFactory), join, 4, deleteOp, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), deleteOp, 0, emptySink4, 0);

        spec.connect(new OneToOneConnectorDescriptor(spec), groupEndOperator, 0, materialize, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), materialize, 0, postSuperStep, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), postSuperStep, 0, emptySink, 0);

        spec.addRoot(terminateWriter);
        spec.addRoot(finalAggregator);
        spec.addRoot(emptySink);

        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy(spec));
        spec.setFrameSize(frameSize);
        return spec;
    }

    @Override
    public JobSpecification[] generateCleanup() throws HyracksException {
        JobSpecification[] cleanups = new JobSpecification[1];
        cleanups[0] = this.dropIndex(PRIMARY_INDEX);
        return cleanups;
    }

}
