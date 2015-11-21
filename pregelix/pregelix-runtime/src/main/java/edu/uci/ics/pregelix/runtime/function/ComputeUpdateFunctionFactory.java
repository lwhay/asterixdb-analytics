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
package edu.uci.ics.pregelix.runtime.function;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.hyracks.api.comm.IFrame;
import edu.uci.ics.hyracks.api.comm.IFrameFieldAppender;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.comm.VSizeFrame;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameFixedFieldTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.pregelix.api.graph.GlobalAggregator;
import edu.uci.ics.pregelix.api.graph.MsgList;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.util.ArrayListWritable.ArrayIterator;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.pregelix.api.util.FrameTupleUtils;
import edu.uci.ics.pregelix.dataflow.base.IConfigurationFactory;
import edu.uci.ics.pregelix.dataflow.std.base.IUpdateFunction;
import edu.uci.ics.pregelix.dataflow.std.base.IUpdateFunctionFactory;
import edu.uci.ics.pregelix.dataflow.std.util.ResetableByteArrayOutputStream;
import edu.uci.ics.pregelix.dataflow.util.IterationUtils;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class ComputeUpdateFunctionFactory implements IUpdateFunctionFactory {
    private static final long serialVersionUID = 1L;
    private final IConfigurationFactory confFactory;

    public ComputeUpdateFunctionFactory(IConfigurationFactory confFactory) {
        this.confFactory = confFactory;
    }

    @Override
    public IUpdateFunction createFunction() {
        return new IUpdateFunction() {
            // for writing intermediate data
            private final ArrayTupleBuilder tbMsg = new ArrayTupleBuilder(2);
            private final ArrayTupleBuilder tbAlive = new ArrayTupleBuilder(2);
            private final ArrayTupleBuilder tbTerminate = new ArrayTupleBuilder(1);
            private final ArrayTupleBuilder tbInsert = new ArrayTupleBuilder(2);
            private final ArrayTupleBuilder tbDelete = new ArrayTupleBuilder(1);
            private ArrayTupleBuilder tbGlobalAggregate;

            // for writing out to message channel
            private IFrameWriter writerMsg;
            private IFrameFieldAppender appenderMsg;
            private IFrame frameMsg;

            // for writing out to alive message channel
            private IFrameWriter writerAlive;
            private IFrameFieldAppender appenderAlive;
            private IFrame frameAlive;
            private boolean pushAlive;

            // for writing out termination detection control channel
            private IFrameWriter writerTerminate;
            private IFrameFieldAppender appenderTerminate;
            private IFrame frameTerminate;
            private boolean terminate = true;

            // for writing out termination detection control channel
            private IFrameWriter writerGlobalAggregate;
            private IFrameFieldAppender appenderGlobalAggregate;
            private IFrame frameGlobalAggregate;
            private List<GlobalAggregator> aggregators;

            // for writing out to insert vertex channel
            private IFrameWriter writerInsert;
            private IFrameFieldAppender appenderInsert;
            private IFrame frameInsert;

            // for writing out to delete vertex channel
            private IFrameWriter writerDelete;
            private IFrameFieldAppender appenderDelete;
            private IFrame frameDelete;

            private Vertex vertex;
            private ResetableByteArrayOutputStream bbos = new ResetableByteArrayOutputStream();
            private DataOutput output = new DataOutputStream(bbos);

            private ArrayIterator msgIterator = new ArrayIterator();
            private final List<IFrameWriter> writers = new ArrayList<IFrameWriter>();
            private final List<IFrameFieldAppender> appenders = new ArrayList<IFrameFieldAppender>();
            private final List<ArrayTupleBuilder> tbs = new ArrayList<ArrayTupleBuilder>();
            private Configuration conf;
            private boolean dynamicStateLength;
            private boolean userConfigured;

            private int vertexSizeLimit = -1;
            private String jobId = "";

            private Vertex addedNewVertex;
            private VLongWritable addedNewVertexId;

            @Override
            public void open(IHyracksTaskContext ctx, RecordDescriptor rd, IFrameWriter... writers)
                    throws HyracksDataException {
                this.conf = confFactory.createConfiguration(ctx);
                //LSM index does not have in-place update
                this.dynamicStateLength = BspUtils.getDynamicVertexValueSize(conf) || BspUtils.useLSM(conf);
                this.aggregators = BspUtils.createGlobalAggregators(conf);
                this.userConfigured = false;
                for (int i = 0; i < aggregators.size(); i++) {
                    this.aggregators.get(i).init();
                }

                this.tbGlobalAggregate = new ArrayTupleBuilder(aggregators.size());

                this.writerMsg = writers[0];
                this.frameMsg = new VSizeFrame(ctx);
                this.appenderMsg = new FrameFixedFieldTupleAppender(2);
                this.appenderMsg.reset(frameMsg, true);
                this.writers.add(writerMsg);
                this.appenders.add(appenderMsg);

                this.writerTerminate = writers[1];
                this.frameTerminate = new VSizeFrame(ctx);
                this.appenderTerminate = new FrameFixedFieldTupleAppender(1);
                this.appenderTerminate.reset(frameTerminate, true);

                this.writerGlobalAggregate = writers[2];
                this.frameGlobalAggregate = new VSizeFrame(ctx);
                this.appenderGlobalAggregate = new FrameFixedFieldTupleAppender(1);
                this.appenderGlobalAggregate.reset(frameGlobalAggregate, true);

                this.writerInsert = writers[3];
                this.frameInsert = new VSizeFrame(ctx);
                this.appenderInsert = new FrameFixedFieldTupleAppender(2);
                this.appenderInsert.reset(frameInsert, true);
                this.writers.add(writerInsert);
                this.appenders.add(appenderInsert);

                this.writerDelete = writers[4];
                this.frameDelete = new VSizeFrame(ctx);
                this.appenderDelete = new FrameFixedFieldTupleAppender(1);
                this.appenderDelete.reset(frameDelete, true);
                this.writers.add(writerDelete);
                this.appenders.add(appenderDelete);

                if (writers.length > 5) {
                    this.writerAlive = writers[5];
                    this.frameAlive = new VSizeFrame(ctx);
                    this.appenderAlive = new FrameFixedFieldTupleAppender(1);
                    this.appenderAlive.reset(frameAlive, true);
                    this.pushAlive = true;
                    this.writers.add(writerAlive);
                    this.appenders.add(appenderAlive);
                }

                tbs.add(tbMsg);
                tbs.add(tbInsert);
                tbs.add(tbDelete);
                tbs.add(tbAlive);

                vertexSizeLimit = Math.min(IterationUtils.getVFrameSize(ctx) / 2 - 32, ctx.getInitialFrameSize() - 32);
                jobId = BspUtils.getJobId(conf);

                addedNewVertex = BspUtils.createVertex(conf);
                addedNewVertex.setVertexContext(IterationUtils.getVertexContext(BspUtils.getJobId(conf), ctx));
            }

            @Override
            public void process(Object[] tuple) throws HyracksDataException {
                // vertex Id, msg content List, vertex Id, vertex
                tbMsg.reset();
                tbAlive.reset();

                if (tuple[3] == null) {
                    if (vertex == null) {
                        vertex = addedNewVertex;
                    }
                    vertex.getEdges().clear();
                    vertex.getMsgList().clear();
                    vertex.reset();
                    WritableComparable vertexId = (WritableComparable) tuple[0];
                    vertex.setVertexId(vertexId);
                    vertex.activate(true);
                } else {
                    vertex = (Vertex) tuple[3];
                }

                if (vertex.isPartitionTerminated()) {
                    vertex.voteToHalt();
                    return;
                }
                vertex.setOutputWriters(writers);
                vertex.setOutputAppenders(appenders);
                vertex.setOutputTupleBuilders(tbs);

                MsgList msgContentList = (MsgList) tuple[1];
                msgContentList.reset(msgIterator);

                if (!msgIterator.hasNext() && vertex.isHalted()) {
                    return;
                }
                if (vertex.isHalted()) {
                    vertex.activate();
                }

                try {
                    if (!userConfigured) {
                        vertex.configure(conf);
                        userConfigured = true;
                    }
                    if (msgContentList.segmentStart()) {
                        vertex.open();
                    }
                    vertex.compute(msgIterator);
                    if (msgContentList.segmentEnd()) {
                        vertex.close();
                    }
                    vertex.finishCompute();
                } catch (Exception e) {
                    ClassLoader cl1 = vertex.getClass().getClassLoader();
                    ClassLoader cl2 = msgContentList.get(0).getClass().getClassLoader();
                    System.out.println("cl1 " + cl1);
                    System.out.println("cl2 " + cl2);
                    throw new HyracksDataException(e);
                }

                /**
                 * this partition should not terminate
                 */
                if (terminate && (!vertex.isHalted() || vertex.hasMessage() || vertex.createdNewLiveVertex())) {
                    terminate = false;
                }

                if (msgContentList.segmentEnd()) {
                    /** the if condition makes sure aggregate only calls once per-vertex */
                    for (int i = 0; i < aggregators.size(); i++) {
                        aggregators.get(i).step(vertex);
                    }
                }
            }

            @Override
            public void close() throws HyracksDataException {
                FrameTupleUtils.flushTuplesFinal(appenderMsg, writerMsg);
                FrameTupleUtils.flushTuplesFinal(appenderInsert, writerInsert);
                FrameTupleUtils.flushTuplesFinal(appenderDelete, writerDelete);

                if (pushAlive) {
                    FrameTupleUtils.flushTuplesFinal(appenderAlive, writerAlive);
                }
                if (!terminate) {
                    writeOutTerminationState();
                }

                /** write out global aggregate value */
                writeOutGlobalAggregate();

                /** end of a superstep, for vertices to release resources */
                if (userConfigured) {
                    vertex.endSuperstep(conf);
                }
            }

            private void writeOutGlobalAggregate() throws HyracksDataException {
                try {
                    for (int i = 0; i < aggregators.size(); i++) {
                        /**
                         * get partial aggregate result and flush to the final
                         * aggregator
                         */
                        Writable agg = aggregators.get(i).finishPartial();
                        agg.write(tbGlobalAggregate.getDataOutput());
                        tbGlobalAggregate.addFieldEndOffset();
                    }
                    FrameTupleUtils.flushTuple(appenderGlobalAggregate, tbGlobalAggregate, writerGlobalAggregate);
                    FrameTupleUtils.flushTuplesFinal(appenderGlobalAggregate, writerGlobalAggregate);
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }

            private void writeOutTerminationState() throws HyracksDataException {
                try {
                    tbTerminate.getDataOutput().writeLong(0);
                    tbTerminate.addFieldEndOffset();
                    FrameTupleUtils.flushTuple(appenderTerminate, tbTerminate, writerTerminate);
                    FrameTupleUtils.flushTuplesFinal(appenderTerminate, writerTerminate);
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }

            @Override
            public void update(ITupleReference tupleRef, ArrayTupleBuilder cloneUpdateTb, IIndexCursor cursor)
                    throws HyracksDataException {
                try {
                    if (vertex != null && vertex.hasUpdate()) {
                        if (!dynamicStateLength && tupleRef != null) {
                            // in-place update
                            byte[] data = tupleRef.getFieldData(1);
                            int offset = tupleRef.getFieldStart(1);
                            bbos.setByteArray(data, offset);
                            vertex.write(output);
                            ITreeIndexCursor tCursor = (ITreeIndexCursor) cursor;
                            tCursor.markCurrentTupleAsUpdated();
                        } else {
                            // write the vertex id
                            DataOutput tbOutput = cloneUpdateTb.getDataOutput();
                            vertex.getVertexId().write(tbOutput);
                            cloneUpdateTb.addFieldEndOffset();
                            // write the vertex value
                            vertex.write(tbOutput);
                            cloneUpdateTb.addFieldEndOffset();

                            // spill large vertex separately
                            if (cloneUpdateTb.getSize() >= vertexSizeLimit) {
                                //spill vertex to HDFS if it cannot fit into a tree storage page
                                String pathStr = BspUtils.TMP_DIR + jobId + File.separator + vertex.getVertexId();
                                vertex.setSpilled(pathStr);
                                cloneUpdateTb.reset();
                                vertex.getVertexId().write(tbOutput);
                                cloneUpdateTb.addFieldEndOffset();
                                //vertex content will be spilled to HDFS
                                vertex.write(tbOutput);
                                cloneUpdateTb.addFieldEndOffset();
                                vertex.setUnSpilled();
                            }
                        }
                    }
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
        };
    }
}
