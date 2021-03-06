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

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.pregelix.dataflow.context.RuntimeContext;

/**
 * Clear the state of the RuntimeContext in one slave
 * 
 * @author yingyib
 */
public class ClearStateOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;
    private String jobId;
    private boolean allStates;

    public ClearStateOperatorDescriptor(JobSpecification spec, String jobId, boolean allStates) {
        super(spec, 0, 0);
        this.jobId = jobId;
        this.allStates = allStates;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return new IOperatorNodePushable() {

            @Override
            public void initialize() throws HyracksDataException {
                RuntimeContext context = (RuntimeContext) ctx.getJobletContext().getApplicationContext()
                        .getApplicationObject();
                context.clearState(jobId, allStates);
                System.gc();
            }

            @Override
            public void deinitialize() throws HyracksDataException {

            }

            @Override
            public int getInputArity() {
                return 0;
            }

            @Override
            public void setOutputFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc)
                    throws HyracksDataException {

            }

            @Override
            public IFrameWriter getInputFrameWriter(int index) {
                return null;
            }

            @Override
            public String getDisplayName() {
                return "Clear State Operator";
            }

        };
    }

}
