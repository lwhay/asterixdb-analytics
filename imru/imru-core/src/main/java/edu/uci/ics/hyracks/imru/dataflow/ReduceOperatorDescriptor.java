/*
 * Copyright 2009-2010 by The Regents of the University of California
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

package edu.uci.ics.hyracks.imru.dataflow;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.Future;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

import edu.uci.ics.hyracks.imru.api.ASyncIO;
import edu.uci.ics.hyracks.imru.api.IIMRUJob2;
import edu.uci.ics.hyracks.imru.api.IMRUReduceContext;
import edu.uci.ics.hyracks.imru.data.MergedFrames;

/**
 * Evaluates the reduce function in an iterative map reduce update job.
 * 
 * @author Josh Rosen
 */
public class ReduceOperatorDescriptor extends IMRUOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private static final RecordDescriptor dummyRecordDescriptor = new RecordDescriptor(new ISerializerDeserializer[1]);

    private final IIMRUJob2<?, ?> imruSpec;
    public boolean isLocal = false;
    public int level = 0;

    /**
     * Create a new ReduceOperatorDescriptor.
     * 
     * @param spec
     *            The job specification
     * @param imruSpec
     *            The IMRU Job specification
     */
    public ReduceOperatorDescriptor(JobSpecification spec, IIMRUJob2<?, ?> imruSpec, String name) {
        super(spec, 1, 1, name, imruSpec);
        this.imruSpec = imruSpec;
        recordDescriptors[0] = dummyRecordDescriptor;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions)
            throws HyracksDataException {
        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
            IMRUReduceContext imruContext;
            //            private final ChunkFrameHelper chunkFrameHelper;
            //            private final List<List<ByteBuffer>> bufferedChunks;
            Hashtable<Integer, LinkedList<ByteBuffer>> hash = new Hashtable<Integer, LinkedList<ByteBuffer>>();
            public String name;
            private ASyncIO<byte[]> io;
            Future future;

            {
                this.name = ReduceOperatorDescriptor.this.getDisplayName() + partition;
                //                this.chunkFrameHelper = new ChunkFrameHelper(ctx);
                //                this.bufferedChunks = new ArrayList<List<ByteBuffer>>();
            }

            @Override
            public void open() throws HyracksDataException {
                writer.open();
                imruContext = new IMRUReduceContext(ctx, name, isLocal, level);
                //                imruContext = new IMRUReduceContext(chunkFrameHelper
                //                        .getContext(), name, isLocal, level);
                //                writer = chunkFrameHelper.wrapWriter(writer, partition);

                io = new ASyncIO<byte[]>(1);
                future = IMRUSerialize.threadPool.submit(new Runnable() {
                    @Override
                    public void run() {
                        Iterator<byte[]> input = io.getInput();
                        ByteArrayOutputStream out = new ByteArrayOutputStream();
                        try {
                            imruSpec.reduce(imruContext, input, out);
                            byte[] objectData = out.toByteArray();
                            //                            Rt.p("reduce send "
                            //                                    + MergedFrames.deserialize(objectData));
                            IMRUDebugger.sendDebugInfo(imruContext.getNodeId() + " reduce start " + partition);
                            MergedFrames.serializeToFrames(
                                    imruContext,
                                    writer,
                                    objectData,
                                    partition,
                                    imruContext.getNodeId() + " reduce " + partition + " "
                                            + imruContext.getOperatorName());
                            IMRUDebugger.sendDebugInfo(imruContext.getNodeId() + " reduce finish");
                            //                            IMRUSerialize.serializeToFrames(imruContext,
                            //                                    writer, objectData);
                        } catch (HyracksDataException e) {
                            e.printStackTrace();
                            try {
                                fail();
                            } catch (HyracksDataException e1) {
                                e1.printStackTrace();
                            }
                        }
                    }
                });
            }

            @Override
            public void nextFrame(ByteBuffer encapsulatedChunk) throws HyracksDataException {
                try {
                    //                    Rt.p("reduce frame");
                    MergedFrames frames = MergedFrames.nextFrame(ctx, encapsulatedChunk, hash, imruContext.getNodeId()
                            + " recv " + partition + " " + imruContext.getOperatorName());
                    if (frames != null) {
                        //                        Rt.p("reduce recv "
                        //                                + MergedFrames.deserialize(frames.data));
                        io.add(frames.data);
                    }
                    //                    ByteBuffer chunk = chunkFrameHelper
                    //                            .extractChunk(encapsulatedChunk);
                    //                    int senderPartition = chunkFrameHelper
                    //                            .getPartition(encapsulatedChunk);
                    //                    boolean isLastChunk = chunkFrameHelper
                    //                            .isLastChunk(encapsulatedChunk);
                    //                    enqueueChunk(chunk, senderPartition);
                    //                    if (isLastChunk) {
                    //                        byte[] data = IMRUSerialize.deserializeFromChunks(
                    //                                imruContext, bufferedChunks
                    //                                        .remove(senderPartition));
                    //                        io.add(data);
                    //                    }
                } catch (HyracksDataException e) {
                    fail();
                    throw e;
                } catch (Throwable e) {
                    fail();
                    throw new HyracksDataException(e);
                }
            }

            @Override
            public void fail() throws HyracksDataException {
                //                writer.fail();
            }

            @Override
            public void close() throws HyracksDataException {
                io.close();
                try {
                    future.get();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                writer.close();
            }

            //            private void enqueueChunk(ByteBuffer chunk, int senderPartition) {
            //                if (bufferedChunks.size() <= senderPartition) {
            //                    for (int i = bufferedChunks.size(); i <= senderPartition; i++) {
            //                        bufferedChunks.add(new LinkedList<ByteBuffer>());
            //                    }
            //                }
            //                bufferedChunks.get(senderPartition).add(chunk);
            //            }

        };
    }
}
