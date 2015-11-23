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

public class IndexNestedLoopSetUnionFunctionUpdateOperatorNodePushable extends AbstractUnaryInputOperatorNodePushable {
    private IndexDataflowHelper treeIndexOpHelper;
    private FrameTupleAccessor accessor;

    private IFrame writeFrame;
    private FrameTupleAppender appender;

    private ITreeIndex index;
    private boolean isForward;
    private RangePredicate rangePred;
    private MultiComparator lowKeySearchCmp;
    private IIndexCursor cursor;
    protected IIndexAccessor indexAccessor;

    private RecordDescriptor recDesc;
    private PermutingFrameTupleReference lowKey;
    private PermutingFrameTupleReference highKey;

    private ITupleReference currentTopTuple;
    private boolean match;

    private final IFrameWriter[] writers;
    private final FunctionProxy functionProxy;
    private ArrayTupleBuilder cloneUpdateTb;
    private UpdateBuffer updateBuffer;
    private final SearchKeyTupleReference tempTupleReference = new SearchKeyTupleReference();
    private final StorageType storageType;

    public IndexNestedLoopSetUnionFunctionUpdateOperatorNodePushable(AbstractTreeIndexOperatorDescriptor opDesc,
            IHyracksTaskContext ctx, int partition, IRecordDescriptorProvider recordDescProvider, boolean isForward,
            int[] lowKeyFields, int[] highKeyFields, IUpdateFunctionFactory functionFactory,
            IRuntimeHookFactory preHookFactory, IRuntimeHookFactory postHookFactory,
            IRecordDescriptorFactory inputRdFactory, int outputArity) throws HyracksDataException {
        treeIndexOpHelper = (IndexDataflowHelper) opDesc.getIndexDataflowHelperFactory().createIndexDataflowHelper(
                opDesc, ctx, partition);
        if (treeIndexOpHelper instanceof TreeIndexDataflowHelper) {
            storageType = StorageType.TreeIndex;
        } else {
            storageType = StorageType.LSMIndex;
        }
        this.isForward = isForward;
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
        functionProxy.functionOpen();
        accessor = new FrameTupleAccessor(recDesc);

        try {
            treeIndexOpHelper.open();
            index = (ITreeIndex) treeIndexOpHelper.getIndexInstance();

            rangePred = new RangePredicate(null, null, true, true, null, null);
            int lowKeySearchFields = index.getComparatorFactories().length;
            IBinaryComparator[] lowKeySearchComparators = new IBinaryComparator[lowKeySearchFields];
            for (int i = 0; i < lowKeySearchFields; i++) {
                lowKeySearchComparators[i] = index.getComparatorFactories()[i].createBinaryComparator();
            }
            lowKeySearchCmp = new MultiComparator(lowKeySearchComparators);

            writeFrame = new VSizeFrame(treeIndexOpHelper.getTaskContext());
            appender = new FrameTupleAppender();
            appender.reset(writeFrame, true);

            indexAccessor = index.createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
            setCursor();

            /** set the search cursor */
            rangePred.setLowKey(null, true);
            rangePred.setHighKey(null, true);
            cursor.reset();
            indexAccessor.search(cursor, rangePred);

            /** set up current top tuple */
            if (cursor.hasNext()) {
                cursor.next();
                currentTopTuple = cursor.getTuple();
                match = false;
            }
            cloneUpdateTb = new ArrayTupleBuilder(index.getFieldCount());
        } catch (Exception e) {
            closeResource();
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);
        int tupleCount = accessor.getTupleCount();
        try {
            for (int i = 0; i < tupleCount;) {
                if (lowKey != null) {
                    lowKey.reset(accessor, i);
                }
                if (highKey != null) {
                    highKey.reset(accessor, i);
                }
                // TODO: currently use low key only, check what they mean
                if (currentTopTuple != null) {
                    int cmp = compare(lowKey, currentTopTuple);
                    if (cmp == 0) {
                        outputMatch(i);
                        i++;
                    } else if ((cmp > 0 && isForward) || (cmp < 0 && !isForward)) {
                        moveTreeCursor();
                    } else {
                        writeLeftResults(accessor, i, null);
                        i++;
                    }
                } else {
                    writeLeftResults(accessor, i, null);
                    i++;
                }
            }
        } catch (Exception e) {
            closeResource();
            throw new HyracksDataException(e);
        }
    }

    private void outputMatch(int i) throws Exception {
        writeLeftResults(accessor, i, currentTopTuple);
        match = true;
    }

    private void moveTreeCursor() throws Exception {
        if (!match) {
            writeRightResults(currentTopTuple);
        }
        if (cursor.hasNext()) {
            cursor.next();
            currentTopTuple = cursor.getTuple();
            match = false;
        } else {
            currentTopTuple = null;
        }
    }

    @Override
    public void close() throws HyracksDataException {
        try {
            while (currentTopTuple != null) {
                moveTreeCursor();
            }
            try {
                cursor.close();

                //batch update
                updateBuffer.updateIndex(indexAccessor);
            } catch (Exception e) {
                throw new HyracksDataException(e);
            }
            functionProxy.functionClose();
        } catch (Exception e) {
            throw new HyracksDataException(e);
        } finally {
            treeIndexOpHelper.close();
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        closeResource();
        populateFailure();
    }

    private void populateFailure() throws HyracksDataException {
        for (IFrameWriter writer : writers) {
            writer.fail();
        }
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

    /** compare tuples */
    private int compare(ITupleReference left, ITupleReference right) throws Exception {
        return lowKeySearchCmp.compare(left, right);
    }

    /** write the right result */
    private void writeRightResults(ITupleReference frameTuple) throws Exception {
        functionProxy.functionCall(frameTuple, cloneUpdateTb, cursor);

        //doing clone update
        CopyUpdateUtil.copyUpdate(tempTupleReference, frameTuple, updateBuffer, cloneUpdateTb, indexAccessor, cursor,
                rangePred, true, storageType);
    }

    /** write the left result */
    private void writeLeftResults(IFrameTupleAccessor leftAccessor, int tIndex, ITupleReference indexTuple)
            throws Exception {
        functionProxy.functionCall(leftAccessor, tIndex, indexTuple, cloneUpdateTb, cursor);

        //doing clone update
        CopyUpdateUtil.copyUpdate(tempTupleReference, indexTuple, updateBuffer, cloneUpdateTb, indexAccessor, cursor,
                rangePred, true, storageType);
    }

    @Override
    public void setOutputFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc) {
        writers[index] = writer;
    }
}
