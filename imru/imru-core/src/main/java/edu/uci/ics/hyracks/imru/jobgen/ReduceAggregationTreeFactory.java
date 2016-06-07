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

package edu.uci.ics.hyracks.imru.jobgen;

import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.constraints.PartitionConstraintHelper;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.connectors.LocalityAwareMToNPartitioningConnectorDescriptor;

import edu.uci.ics.hyracks.imru.api.IIMRUJob2;
import edu.uci.ics.hyracks.imru.dataflow.ReduceOperatorDescriptor;

/**
 * Constructs aggregation trees between the Map and Update operators.
 */
public class ReduceAggregationTreeFactory {

    /**
     * Construct an aggregation tree with the specified fan-in.
     * 
     * @param spec
     *            The JobSpecification.
     * @param producerOp
     *            The producer of the outputs being aggregated.
     * @param producerPort
     *            The producer port that connects to the agg. tree
     * @param producerOpCount
     *            The number of producer partitions.
     * @param consumerOp
     *            The consumer of the aggregate.
     * @param consumerPort
     *            The consumer port that connects to the agg. tree.
     * @param consumerConn
     *            Connects the consumer to the agg. tree.
     * @param fanIn
     *            The number of incoming connections to each
     *            aggregator node.
     * @param useLocalCombiners
     *            If true, a layer of local combiners will be added
     *            before the tree.
     * @param producerOpLocations
     *            Maps from producer partitions to NC names. Must be
     *            specified if using local combiners; otherwise, it
     *            can be null.
     * @param imruSpec
     *            The IMRU job specification.
     */
    @SuppressWarnings({ "rawtypes" })
    public static void buildAggregationTree(JobSpecification spec, IOperatorDescriptor producerOp, int producerPort,
            int producerOpCount, IOperatorDescriptor consumerOp, int consumerPort, IConnectorDescriptor consumerConn,
            int fanIn, boolean useLocalCombiners, String[] producerOpLocations, IIMRUJob2 imruSpec) {
        if (useLocalCombiners) {
            producerOpCount = LocalReducerFactory.getReducerCount(producerOpLocations);
        }
        Integer[] levelNodeCounts = aggregationTreeNodeCounts(producerOpCount, fanIn);
        int numLevels = levelNodeCounts.length;
        ReduceOperatorDescriptor[] aggregatorOperators = new ReduceOperatorDescriptor[numLevels];
        for (int level = 0; level < numLevels; level++) {
            aggregatorOperators[level] = new ReduceOperatorDescriptor(spec, imruSpec, "NAryReducerL" + level + "_");
            aggregatorOperators[level].level = level;
            //            aggregatorOperators[level].setDisplayName("ReduceOperatorDescriptor(level " + level + ")");
            PartitionConstraintHelper.addPartitionCountConstraint(spec, aggregatorOperators[level],
                    levelNodeCounts[levelNodeCounts.length - 1 - level]);
        }
        for (int level = 1; level < numLevels; level++) {
            IConnectorDescriptor conn = new LocalityAwareMToNPartitioningConnectorDescriptor(spec,
                    OneToOneTuplePartitionComputerFactory.INSTANCE, new RangeLocalityMap(
                            levelNodeCounts[levelNodeCounts.length - 1 - level]));
            conn.setDisplayName("AggregationTreeLevel" + level + "To" + (level - 1) + "Connector");
            spec.connect(conn, aggregatorOperators[level], 0, aggregatorOperators[level - 1], 0);
        }
        IConnectorDescriptor producerConn = new LocalityAwareMToNPartitioningConnectorDescriptor(spec,
                OneToOneTuplePartitionComputerFactory.INSTANCE, new RangeLocalityMap(producerOpCount));
        if (useLocalCombiners) {
            LocalReducerFactory.addLocalReducers(spec, producerOp, producerPort, producerOpLocations,
                    aggregatorOperators[numLevels - 1], 0, producerConn, imruSpec);
        } else {
            spec.connect(producerConn, producerOp, producerPort, aggregatorOperators[numLevels - 1], 0);
        }
        spec.connect(consumerConn, aggregatorOperators[0], 0, consumerOp, consumerPort);
    }

    /**
     * Determines the number of nodes at each level of an aggregation
     * tree.
     * 
     * @param size
     *            The number of leaf nodes
     * @param fanIn
     *            The fan-in of the aggregation tree
     * @return The number of nodes at each level, from the leaves to
     *         the root (but not including the root).
     */
    public static Integer[] aggregationTreeNodeCounts(int size, int fanIn) {
        if (size == 1)
            return new Integer[] { 1 };
        List<Integer> levels = new ArrayList<Integer>();
        while (size > 1) {
            size = (int) Math.round(Math.ceil((1.0 * size) / fanIn));
            levels.add(size);
        }
        return levels.toArray(new Integer[] {});
    }
}
