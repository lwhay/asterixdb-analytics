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

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * A TuplePartitionComputerFactory that always sends tuples to partition 0.
 * For use with LocalityAwareMToNPartitioningConnectorDescriptor.
 */
public class OneToOneTuplePartitionComputerFactory implements ITuplePartitionComputerFactory {
    private static final long serialVersionUID = 1L;
    public static final OneToOneTuplePartitionComputerFactory INSTANCE = new OneToOneTuplePartitionComputerFactory();

    @Override
    public ITuplePartitionComputer createPartitioner() {
        return new ITuplePartitionComputer() {

            @Override
            public int partition(IFrameTupleAccessor accessor, int tIndex, int nParts) throws HyracksDataException {
                assert nParts == 1;
                return 0;
            }
        };
    }
}
