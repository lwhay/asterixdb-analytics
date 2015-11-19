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
package edu.uci.ics.pregelix.dataflow.std;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputOperatorNodePushable;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.api.IIndexAccessor;
import org.apache.hyracks.storage.am.common.api.IIndexCursor;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.dataflow.AbstractTreeIndexOperatorDescriptor;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelper;
import org.apache.hyracks.storage.am.common.dataflow.TreeIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.common.ophelpers.MultiComparator;
import org.apache.hyracks.storage.am.common.tuples.PermutingFrameTupleReference;
import edu.uci.ics.pregelix.dataflow.std.base.IRecordDescriptorFactory;
import edu.uci.ics.pregelix.dataflow.std.base.IRuntimeHookFactory;
import edu.uci.ics.pregelix.dataflow.std.base.IUpdateFunctionFactory;
import edu.uci.ics.pregelix.dataflow.std.util.CopyUpdateUtil;
import edu.uci.ics.pregelix.dataflow.std.util.FunctionProxy;
import edu.uci.ics.pregelix.dataflow.std.util.SearchKeyTupleReference;
import edu.uci.ics.pregelix.dataflow.std.util.StorageType;
import edu.uci.ics.pregelix.dataflow.std.util.UpdateBuffer;

public class IndexNestedLoopJoinFunctionUpdateOperatorNodePushable extends AbstractUnaryInputOperatorNodePushable {
    private IndexDataflowHelper treeIndexOpHelper;
    private FrameTupleAccessor accessor;

    private IFrame writeFrame;
    private FrameTupleAppender appender;
    private ITreeIndex index;
    private PermutingFrameTupleReference lowKey;
    private PermutingFrameTupleReference highKey;
    private boolean lowKeyInclusive;
    private boolean highKeyInclusive;
    private RangePredicate rangePred;
    private MultiComparator lowKeySearchCmp;
    private MultiComparator highKeySearchCmp;
    private IIndexCursor cursor;
    protected IIndexAccessor indexAccessor;

    private RecordDescriptor recDesc;
    private final IFrameWriter[] writers;
    private final FunctionProxy functionProxy;
    private ArrayTupleBuilder cloneUpdateTb;
    private final UpdateBuffer updateBuffer;
    private final SearchKeyTupleReference tempTupleReference = new SearchKeyTupleReference();
    private final StorageType storageType;

    public IndexNestedLoopJoinFunctionUpdateOperatorNodePushable(AbstractTreeIndexOperatorDescriptor opDesc,
            IHyracksTaskContext ctx, int partition, IRecordDescriptorProvider recordDescProvider, boolean isForward,
            int[] lowKeyFields, int[] highKeyFields, boolean lowKeyInclusive, boolean highKeyInclusive,
            IUpdateFunctionFactory functionFactory, IRuntimeHookFactory preHookFactory,
            IRuntimeHookFactory postHookFactory, IRecordDescriptorFactory inputRdFactory, int outputArity)
            throws HyracksDataException {
        treeIndexOpHelper = (IndexDataflowHelper) opDesc.getIndexDataflowHelperFactory().createIndexDataflowHelper(
                opDesc, ctx, partition);
        if (treeIndexOpHelper instanceof TreeIndexDataflowHelper) {
            storageType = StorageType.TreeIndex;
        } else {
            storageType = StorageType.LSMIndex;
        }
        this.lowKeyInclusive = lowKeyInclusive;
        this.highKeyInclusive = highKeyInclusive;
        this.recDesc = recordDescProvider.getInputRecordDescriptor(opDesc.getActivityId(), 0);
        if (lowKeyFields != null && lowKeyFields.length > 0) {
            lowKey = new PermutingFrameTupleReference();
            lowKey.setFieldPermutation(lowKeyFields);
        }
        if (highKeyFields != null && highKeyFields.length > 0) {
            highKey = new PermutingFrameTupleReference();
            highKey.setFieldPermutation(highKeyFields);
        }

        this.writers = new IFrameWriter[outputArity];
        this.functionProxy = new FunctionProxy(ctx, functionFactory, preHookFactory, postHookFactory, inputRdFactory,
                writers);
        this.updateBuffer = new UpdateBuffer(ctx);
    }

    protected void setCursor() {
        cursor = indexAccessor.createSearchCursor(true);
    }

    @Override
    public void open() throws HyracksDataException {
        /**
         * open the function
         */
        functionProxy.functionOpen();
        accessor = new FrameTupleAccessor(recDesc);

        try {
            treeIndexOpHelper.open();
            index = (ITreeIndex) treeIndexOpHelper.getIndexInstance();

            // TODO: Can we construct the multicmps using helper methods?
            int lowKeySearchFields = index.getComparatorFactories().length;
            int highKeySearchFields = index.getComparatorFactories().length;
            if (lowKey != null) {
                lowKeySearchFields = lowKey.getFieldCount();
            }
            if (highKey != null) {
                highKeySearchFields = highKey.getFieldCount();
            }

            IBinaryComparator[] lowKeySearchComparators = new IBinaryComparator[lowKeySearchFields];
            for (int i = 0; i < lowKeySearchFields; i++) {
                lowKeySearchComparators[i] = index.getComparatorFactories()[i].createBinaryComparator();
            }
            lowKeySearchCmp = new MultiComparator(lowKeySearchComparators);

            if (lowKeySearchFields == highKeySearchFields) {
                highKeySearchCmp = lowKeySearchCmp;
            } else {
                IBinaryComparator[] highKeySearchComparators = new IBinaryComparator[highKeySearchFields];
                for (int i = 0; i < highKeySearchFields; i++) {
                    highKeySearchComparators[i] = index.getComparatorFactories()[i].createBinaryComparator();
                }
                highKeySearchCmp = new MultiComparator(highKeySearchComparators);
            }

            rangePred = new RangePredicate(null, null, lowKeyInclusive, highKeyInclusive, lowKeySearchCmp,
                    highKeySearchCmp);
            writeFrame = new VSizeFrame(treeIndexOpHelper.getTaskContext());
            appender = new FrameTupleAppender();
            appender.reset(writeFrame, true);

            indexAccessor = index.createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
            setCursor();
            cloneUpdateTb = new ArrayTupleBuilder(index.getFieldCount());
        } catch (Exception e) {
            closeResource();
            throw new HyracksDataException(e);
        }
    }

    private void writeSearchResults(IFrameTupleAccessor leftAccessor, int tIndex) throws Exception {
        while (cursor.hasNext()) {
            cursor.next();
            ITupleReference tupleRef = cursor.getTuple();

            /**
             * merge with updated tuple
             */
            ITupleReference indexEntryTuple = tupleRef;
            ITupleReference cachedUpdatedLastTuple = updateBuffer.getLastTuple();
            if (cachedUpdatedLastTuple != null) {
                if (compare(cachedUpdatedLastTuple, tupleRef) == 0) {
                    indexEntryTuple = cachedUpdatedLastTuple;
                }
            }

            /**
             * call the update function
             */
            functionProxy.functionCall(leftAccessor, tIndex, indexEntryTuple, cloneUpdateTb, cursor);

            /**
             * doing copy update
             */
            CopyUpdateUtil.copyUpdate(tempTupleReference, indexEntryTuple, updateBuffer, cloneUpdateTb, indexAccessor,
                    cursor, rangePred, false, storageType);
        }
    }

    /** compare tuples */
    private int compare(ITupleReference left, ITupleReference right) throws Exception {
        return lowKeySearchCmp.compare(left, right);
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);

        int tupleCount = accessor.getTupleCount();
        try {
            for (int i = 0; i < tupleCount; i++) {
                if (lowKey != null) {
                    lowKey.reset(accessor, i);
                }
                if (highKey != null) {
                    highKey.reset(accessor, i);
                }
                rangePred.setLowKey(lowKey, lowKeyInclusive);
                rangePred.setHighKey(highKey, highKeyInclusive);

                cursor.reset();
                indexAccessor.search(cursor, rangePred);
                writeSearchResults(accessor, i);
            }
        } catch (Exception e) {
            closeResource();
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void close() throws HyracksDataException {
        try {
            cursor.close();
            //batch update
            updateBuffer.updateIndex(indexAccessor);
        } catch (Exception e) {
            closeResource();
            throw new HyracksDataException(e);
        } finally {
            treeIndexOpHelper.close();
            /**
             * close the update function
             */
            functionProxy.functionClose();
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        closeResource();
        populateFailure();
    }

    private void closeResource() throws HyracksDataException {
        try {
            cursor.close();
        } catch (Exception e) {
            throw new HyracksDataException(e);
        } finally {
            treeIndexOpHelper.close();
        }
    }

    private void populateFailure() throws HyracksDataException {
        for (IFrameWriter writer : writers) {
            writer.fail();
        }
    }

    @Override
    public void setOutputFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc) {
        writers[index] = writer;
    }

}
