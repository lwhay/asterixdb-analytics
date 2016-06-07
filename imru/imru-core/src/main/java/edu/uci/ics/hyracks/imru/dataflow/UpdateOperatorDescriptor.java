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

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;

import edu.uci.ics.hyracks.imru.api.ASyncIO;
import edu.uci.ics.hyracks.imru.api.IIMRUJob2;
import edu.uci.ics.hyracks.imru.api.IMRUContext;
import edu.uci.ics.hyracks.imru.data.MergedFrames;
import edu.uci.ics.hyracks.imru.runtime.bootstrap.IMRUConnection;
import edu.uci.ics.hyracks.imru.util.MemoryStatsLogger;
import edu.uci.ics.hyracks.imru.util.Rt;

/**
 * Evaluates the update function in an iterative map reduce update
 * job.
 * <p>
 * The updated model is serialized to a file in HDFS, where it is read by the driver and mappers.
 * 
 * @param <Model>
 *            Josh Rosen
 */
public class UpdateOperatorDescriptor<Model extends Serializable, Data extends Serializable> extends
        IMRUOperatorDescriptor<Model, Data> {

    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger.getLogger(UpdateOperatorDescriptor.class.getName());
    private static final RecordDescriptor dummyRecordDescriptor = new RecordDescriptor(new ISerializerDeserializer[1]);

    private final String modelName;
    IMRUConnection imruConnection;

    /**
     * Create a new UpdateOperatorDescriptor.
     * 
     * @param spec
     *            The job specification
     * @param imruSpec
     *            The IMRU job specification
     * @param modelInPath
     *            The HDFS path to read the current model from
     * @param confFactory
     *            A Hadoop configuration, used for HDFS.
     * @param envOutPath
     *            The HDFS path to serialize the updated environment
     *            to.
     */
    public UpdateOperatorDescriptor(JobSpecification spec, IIMRUJob2<Model, Data> imruSpec, String modelName,
            IMRUConnection imruConnection) {
        super(spec, 1, 0, "update", imruSpec);
        this.modelName = modelName;
        this.imruConnection = imruConnection;
        //            recordDescriptors[0] = dummyRecordDescriptor;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions)
            throws HyracksDataException {
        return new AbstractUnaryInputSinkOperatorNodePushable() {
            //            private final ChunkFrameHelper chunkFrameHelper;
            //            private final List<List<ByteBuffer>> bufferedChunks;
            Hashtable<Integer, LinkedList<ByteBuffer>> hash = new Hashtable<Integer, LinkedList<ByteBuffer>>();
            private Model model;
            private final String name;
            IMRUContext imruContext;

            private ASyncIO<byte[]> io;
            Future future;
            Model updatedModel;

            {
                this.name = UpdateOperatorDescriptor.this.getDisplayName() + partition;
                //                this.chunkFrameHelper = new ChunkFrameHelper(ctx);
                //                this.bufferedChunks = new ArrayList<List<ByteBuffer>>();
                //                imruContext = new IMRUContext(chunkFrameHelper.getContext(),
                //                        name);
                imruContext = new IMRUContext(ctx, name);
            }

            @SuppressWarnings("unchecked")
            @Override
            public void open() throws HyracksDataException {
                MemoryStatsLogger.logHeapStats(LOG, "Update: Initializing Update");
                model = (Model) imruContext.getModel();
                if (model == null)
                    Rt.p("Model == null " + imruContext.getNodeId());
                io = new ASyncIO<byte[]>();
                future = IMRUSerialize.threadPool.submit(new Runnable() {
                    @Override
                    public void run() {
                        Iterator<byte[]> input = io.getInput();
                        try {
                            updatedModel = imruSpec.update(imruContext, input, model);
                        } catch (HyracksDataException e) {
                            e.printStackTrace();
                        }
                    }
                });
            }

            @Override
            public void nextFrame(ByteBuffer encapsulatedChunk) throws HyracksDataException {
                //                Rt.p("update frame");
                MergedFrames frames = MergedFrames.nextFrame(ctx, encapsulatedChunk, hash, imruContext.getNodeId()
                        + " recv " + partition + " " + imruContext.getOperatorName());
                if (frames != null) {
                    //                    Rt.p("update recv "
                    //                            + MergedFrames.deserialize(frames.data));
                    io.add(frames.data);
                }

                //                ByteBuffer chunk = chunkFrameHelper
                //                        .extractChunk(encapsulatedChunk);
                //                int senderPartition = chunkFrameHelper
                //                        .getPartition(encapsulatedChunk);
                //                boolean isLastChunk = chunkFrameHelper
                //                        .isLastChunk(encapsulatedChunk);
                //                enqueueChunk(chunk, senderPartition);
                //                if (isLastChunk) {
                //                    byte[] data = IMRUSerialize
                //                            .deserializeFromChunks(imruContext, bufferedChunks
                //                                    .remove(senderPartition));
                //                    if (data.length == 0)
                //                        throw new HyracksDataException(
                //                                "update received empty data");
                //                    io.add(data);
                //                }
            }

            @Override
            public void fail() throws HyracksDataException {
            }

            @Override
            public void close() throws HyracksDataException {
                try {
                    io.close();
                    try {
                        future.get();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    model = (Model) updatedModel;
                    imruContext.setModel(model);

                    long start = System.currentTimeMillis();
                    imruConnection.uploadModel(modelName, model);
                    long end = System.currentTimeMillis();
                    //                Rt.p(model);
                    LOG.info("uploaded model to CC " + (end - start) + " milliseconds");
                    MemoryStatsLogger.logHeapStats(LOG, "Update: Deinitializing Update");
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
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
