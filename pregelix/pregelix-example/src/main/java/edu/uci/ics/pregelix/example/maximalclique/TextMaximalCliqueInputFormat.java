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

package edu.uci.ics.pregelix.example.maximalclique;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.io.VertexReader;
import edu.uci.ics.pregelix.api.io.text.TextVertexInputFormat;
import edu.uci.ics.pregelix.api.io.text.TextVertexInputFormat.TextVertexReader;
import edu.uci.ics.pregelix.api.util.BspUtils;

public class TextMaximalCliqueInputFormat extends
        TextVertexInputFormat<VLongWritable, CliquesWritable, NullWritable, AdjacencyListWritable> {

    @Override
    public VertexReader<VLongWritable, CliquesWritable, NullWritable, AdjacencyListWritable> createVertexReader(
            InputSplit split, TaskAttemptContext context) throws IOException {
        return new TextMaximalCliqueGraphReader(textInputFormat.createRecordReader(split, context));
    }
}

@SuppressWarnings("rawtypes")
class TextMaximalCliqueGraphReader extends
        TextVertexReader<VLongWritable, CliquesWritable, NullWritable, AdjacencyListWritable> {

    private final static String separator = " ";
    private Vertex vertex;
    private VLongWritable vertexId = new VLongWritable();
    private List<VLongWritable> pool = new ArrayList<VLongWritable>();
    private int used = 0;

    public TextMaximalCliqueGraphReader(RecordReader<LongWritable, Text> lineRecordReader) {
        super(lineRecordReader);
    }

    @Override
    public boolean nextVertex() throws IOException, InterruptedException {
        return getRecordReader().nextKeyValue();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Vertex<VLongWritable, CliquesWritable, NullWritable, AdjacencyListWritable> getCurrentVertex()
            throws IOException, InterruptedException {
        used = 0;
        if (vertex == null) {
            vertex = BspUtils.createVertex(getContext().getConfiguration());
        }
        vertex.getMsgList().clear();
        vertex.getEdges().clear();

        vertex.reset();
        Text line = getRecordReader().getCurrentValue();
        String[] fields = line.toString().split(separator);

        if (fields.length > 0) {
            /**
             * set the src vertex id
             */
            long src = Long.parseLong(fields[0]);
            vertexId.set(src);
            vertex.setVertexId(vertexId);
            long dest = -1L;

            /**
             * set up edges
             */
            for (int i = 1; i < fields.length; i++) {
                dest = Long.parseLong(fields[i]);
                VLongWritable destId = allocate();
                destId.set(dest);
                vertex.addEdge(destId, null);
            }
        }
        return vertex;
    }

    private VLongWritable allocate() {
        if (used >= pool.size()) {
            VLongWritable value = new VLongWritable();
            pool.add(value);
            used++;
            return value;
        } else {
            VLongWritable value = pool.get(used);
            used++;
            return value;
        }
    }
}
