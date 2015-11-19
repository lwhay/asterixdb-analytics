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
package edu.uci.ics.pregelix.dataflow;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import org.apache.hyracks.dataflow.std.file.FileSplit;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.storage.common.file.LocalResource;
import edu.uci.ics.pregelix.dataflow.context.RuntimeContext;

/**
 * This Class is intended for simulating an Asterix pipeline entry point
 */
public class FakeMetaOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private IFileSplitProvider fileSplitToAdd;

    public FakeMetaOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor internalRecordDescriptor,
            IFileSplitProvider fileSplitToAdd) {
        super(spec, 0, 1);
        this.recordDescriptors[0] = internalRecordDescriptor;
        this.fileSplitToAdd = fileSplitToAdd;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            final IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {

        try {
            long randomId;
            do {
                randomId = Math.round(1000000 + (Math.random() * 100000));
            } while (RuntimeContext.get(ctx).getLocalResourceRepository().getResourceById(randomId) != null);

            for (FileSplit f : fileSplitToAdd.getFileSplits()) {
                String name = f.getLocalFile().getFile().getAbsolutePath();
                if (RuntimeContext.get(ctx).getLocalResourceRepository().getResourceByName(name + "/device_id_0") == null) {
                    RuntimeContext.get(ctx).getLocalResourceRepository()
                            .insert(new LocalResource(randomId++, name + "/device_id_0", 0, 0, null));
                }
            }
        } catch (HyracksDataException e) {
            e.printStackTrace();
        }

        return createSourceInputPushRuntime(ctx, recordDescProvider, partition, nPartitions);
    }

    private IOperatorNodePushable createSourceInputPushRuntime(final IHyracksTaskContext ctx,
            final IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
        return new AbstractUnaryOutputSourceOperatorNodePushable() {

            @Override
            public void initialize() throws HyracksDataException {
                IFrame frame = new VSizeFrame(ctx);
                ArrayTupleBuilder tb = new ArrayTupleBuilder(0);
                FrameTupleAppender appender = new FrameTupleAppender();

                writer.open();
                appender.reset(frame, true);
                appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());
                FrameUtils.flushFrame(frame.getBuffer(), writer);
                writer.close();
            }
        };
    }
}
