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

package org.apache.hyracks.imru.runtime.bootstrap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Vector;

import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.base.AbstractStateObject;

/**
 * Used to share state (the cached data file) between the data loading
 * and map operators.
 */
public class MapTaskState extends AbstractStateObject {
    private RunFileWriter out;
    private Vector vector;

    public MapTaskState(JobId jobId, TaskId taskId) {
        super(jobId, taskId);
    }

    @Override
    public void toBytes(DataOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void fromBytes(DataInput in) throws IOException {
        throw new UnsupportedOperationException();
    }

    public RunFileWriter getRunFileWriter() {
        return out;
    }

    public void setRunFileWriter(RunFileWriter out) {
        this.out = out;
    }

    public void setMemCache(Vector vector) {
        this.vector = vector;
    }

    public Vector getMemCache() {
        return vector;
    }
}