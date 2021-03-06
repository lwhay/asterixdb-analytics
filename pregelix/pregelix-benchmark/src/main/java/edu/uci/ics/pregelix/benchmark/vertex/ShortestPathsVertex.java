/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.pregelix.benchmark.vertex;

import java.io.IOException;

import org.apache.giraph.combiner.Combiner;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.VLongWritable;

/**
 * Shortest paths algorithm.
 */
public class ShortestPathsVertex extends Vertex<VLongWritable, DoubleWritable, DoubleWritable, DoubleWritable> {
    /** Source id. */
    public static final String SOURCE_ID = "giraph.shortestPathsBenchmark.sourceId";
    /** Default source id. */
    public static final long SOURCE_ID_DEFAULT = 1;

    private boolean isSource() {
        return getId().get() == getConf().getLong(SOURCE_ID, SOURCE_ID_DEFAULT);
    }

    @Override
    public void compute(Iterable<DoubleWritable> messages) throws IOException {
        if (getSuperstep() == 0) {
            setValue(new DoubleWritable(Double.MAX_VALUE));
        }

        double minDist = isSource() ? 0d : Double.MAX_VALUE;
        for (DoubleWritable message : messages) {
            minDist = Math.min(minDist, message.get());
        }

        if (minDist < getValue().get()) {
            setValue(new DoubleWritable(minDist));
            for (Edge<VLongWritable, DoubleWritable> edge : getEdges()) {
                double distance = minDist + edge.getValue().get();
                sendMessage(edge.getTargetVertexId(), new DoubleWritable(distance));
            }
        }

        voteToHalt();
    }

    public static class MinCombiner extends Combiner<VLongWritable, DoubleWritable> {

        @Override
        public void combine(VLongWritable vertexIndex, DoubleWritable originalMessage, DoubleWritable messageToCombine) {
            double oldValue = messageToCombine.get();
            double newValue = originalMessage.get();
            if (newValue < oldValue) {
                messageToCombine.set(newValue);
            }
        }

        @Override
        public DoubleWritable createInitialMessage() {
            return new DoubleWritable(Integer.MAX_VALUE);
        }

    }
}
