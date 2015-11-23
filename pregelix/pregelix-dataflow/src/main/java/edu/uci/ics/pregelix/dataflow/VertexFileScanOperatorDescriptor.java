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

import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import org.apache.hyracks.hdfs.ContextFactory;
import org.apache.hyracks.hdfs2.dataflow.FileSplitsFactory;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.io.VertexInputFormat;
import edu.uci.ics.pregelix.api.io.VertexReader;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.pregelix.dataflow.base.IConfigurationFactory;
import edu.uci.ics.pregelix.dataflow.util.IterationUtils;

@SuppressWarnings("rawtypes")
public class VertexFileScanOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;
    private final FileSplitsFactory splitsFactory;
    private final IConfigurationFactory confFactory;
    private final int fieldSize = 2;
    private final String[] scheduledLocations;
    private final boolean[] executed;

    /**
     * @param spec
     */
    public VertexFileScanOperatorDescriptor(JobSpecification spec, RecordDescriptor rd, List<InputSplit> splits,
            String[] scheduledLocations, IConfigurationFactory confFactory) throws HyracksException {
        super(spec, 0, 1);
        List<FileSplit> fileSplits = new ArrayList<FileSplit>();
        for (int i = 0; i < splits.size(); i++) {
            fileSplits.add((FileSplit) splits.get(i));
        }
        this.splitsFactory = new FileSplitsFactory(fileSplits);
        this.confFactory = confFactory;
        this.scheduledLocations = scheduledLocations;
        this.executed = new boolean[scheduledLocations.length];
        Arrays.fill(executed, false);
        this.recordDescriptors[0] = rd;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, final int partition, final int nPartitions)
            throws HyracksDataException {
        final List<FileSplit> splits = splitsFactory.getSplits();

        return new AbstractUnaryOutputSourceOperatorNodePushable() {
            private final ContextFactory ctxFactory = new ContextFactory();
            private String jobId;

            @Override
            public void initialize() throws HyracksDataException {
                try {
                    Configuration conf = confFactory.createConfiguration(ctx);

                    //get the info for spilling vertices to HDFS
                    jobId = BspUtils.getJobId(conf);

                    writer.open();
                    for (int i = 0; i < scheduledLocations.length; i++) {
                        if (scheduledLocations[i].equals(ctx.getJobletContext().getApplicationContext().getNodeId())) {
                            /**
                             * pick one from the FileSplit queue
                             */
                            synchronized (executed) {
                                if (!executed[i]) {
                                    executed[i] = true;
                                } else {
                                    continue;
                                }
                            }
                            loadVertices(ctx, conf, i);
                        }
                    }
                    writer.close();
                } catch (Exception e) {
                    throw new HyracksDataException(e);
                }
            }

            /**
             * Load the vertices
             *
             * @parameter IHyracks ctx
             * @throws IOException
             * @throws IllegalAccessException
             * @throws InstantiationException
             * @throws ClassNotFoundException
             * @throws InterruptedException
             */
            @SuppressWarnings("unchecked")
            private void loadVertices(final IHyracksTaskContext ctx, Configuration conf, int splitId)
                    throws IOException, ClassNotFoundException, InterruptedException, InstantiationException,
                    IllegalAccessException, NoSuchFieldException, InvocationTargetException {
                int treeVertexSizeLimit = IterationUtils.getVFrameSize(ctx) / 2 - 32;
                IFrame frame = new VSizeFrame(ctx);
                FrameTupleAppender appender = new FrameTupleAppender();
                appender.reset(frame, true);

                VertexInputFormat vertexInputFormat = BspUtils.createVertexInputFormat(conf);
                InputSplit split = splits.get(splitId);
                TaskAttemptContext mapperContext = ctxFactory.createContext(conf, splitId);
                mapperContext.getConfiguration().setClassLoader(ctx.getJobletContext().getClassLoader());

                VertexReader vertexReader = vertexInputFormat.createVertexReader(split, mapperContext);
                vertexReader.initialize(split, mapperContext);
                Vertex readerVertex = BspUtils.createVertex(mapperContext.getConfiguration());
                ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldSize);
                DataOutput dos = tb.getDataOutput();

                IterationUtils.setJobContext(BspUtils.getJobId(conf), ctx, mapperContext);
                Vertex.taskContext = mapperContext;

                /**
                 * empty vertex value
                 */
                Writable emptyVertexValue = BspUtils.createVertexValue(conf);

                while (vertexReader.nextVertex()) {
                    readerVertex = vertexReader.getCurrentVertex();
                    tb.reset();
                    if (readerVertex.getVertexId() == null) {
                        throw new IllegalArgumentException("loadVertices: Vertex reader returned a vertex "
                                + "without an id!  - " + readerVertex);
                    }
                    if (readerVertex.getVertexValue() == null) {
                        readerVertex.setVertexValue(emptyVertexValue);
                    }
                    WritableComparable vertexId = readerVertex.getVertexId();
                    vertexId.write(dos);
                    tb.addFieldEndOffset();

                    readerVertex.write(dos);
                    tb.addFieldEndOffset();

                    if (tb.getSize() >= treeVertexSizeLimit) {
                        //if (tb.getSize() < dataflowPageSize) {
                        //spill vertex to HDFS if it cannot fit into a tree storage page
                        String pathStr = BspUtils.TMP_DIR + jobId + File.separator + vertexId;
                        readerVertex.setSpilled(pathStr);
                        tb.reset();
                        vertexId.write(dos);
                        tb.addFieldEndOffset();
                        //vertex content will be spilled to HDFS
                        readerVertex.write(dos);
                        tb.addFieldEndOffset();
                        readerVertex.setUnSpilled();
                    }
                    FrameUtils.appendToWriter(writer, appender, tb.getFieldEndOffsets(), tb.getByteArray(), 0,
                            tb.getSize());
                }

                vertexReader.close();
                if (appender.getTupleCount() > 0) {
                    FrameUtils.flushFrame(frame.getBuffer(), writer);
                }
                System.gc();
            }
        };
    }
}