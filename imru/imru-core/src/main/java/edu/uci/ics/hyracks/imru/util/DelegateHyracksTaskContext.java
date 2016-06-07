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

package edu.uci.ics.hyracks.imru.util;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;

import org.apache.hyracks.api.context.IHyracksJobletContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.TaskAttemptId;
import org.apache.hyracks.api.dataflow.state.IStateObject;
import org.apache.hyracks.api.dataset.IDatasetPartitionManager;
import org.apache.hyracks.api.deployment.DeploymentId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.job.profiling.counters.ICounterContext;
import org.apache.hyracks.api.resources.IDeallocatable;

/**
 * Base class for classes that specialize an existing
 * IHyracksTaskContext implementation, because many
 * implementations are not subclassable.
 */
public class DelegateHyracksTaskContext implements IHyracksTaskContext {

    private final IHyracksTaskContext delegate;

    /**
     * Construct a new DelegateHyracksTaskContext.
     *
     * @param delegate
     *            The task context to delegate calls to.
     */
    public DelegateHyracksTaskContext(IHyracksTaskContext delegate) {
        this.delegate = delegate;
    }

    @Override
    public ByteBuffer allocateFrame() throws HyracksDataException {
        return delegate.allocateFrame();
    }

    @Override
    public void deallocateFrames(int arg0) {
        delegate.deallocateFrames(arg0);
    }

    @Override
    public int getInitialFrameSize() {
        return delegate.getInitialFrameSize();
    }

    @Override
    public IIOManager getIOManager() {
        return delegate.getIOManager();
    }

    @Override
    public FileReference createUnmanagedWorkspaceFile(String prefix) throws HyracksDataException {
        return delegate.createUnmanagedWorkspaceFile(prefix);
    }

    @Override
    public FileReference createManagedWorkspaceFile(String prefix) throws HyracksDataException {
        return delegate.createManagedWorkspaceFile(prefix);
    }

    @Override
    public void registerDeallocatable(IDeallocatable deallocatable) {
        delegate.registerDeallocatable(deallocatable);
    }

    @Override
    public void setStateObject(IStateObject taskState) {
        delegate.setStateObject(taskState);
    }

    @Override
    public IStateObject getStateObject(Object taskId) {
        return delegate.getStateObject(taskId);
    }

    @Override
    public IHyracksJobletContext getJobletContext() {
        return delegate.getJobletContext();
    }

    @Override
    public TaskAttemptId getTaskAttemptId() {
        return delegate.getTaskAttemptId();
    }

    @Override
    public ICounterContext getCounterContext() {
        return delegate.getCounterContext();
    }

    @Override
    public IDatasetPartitionManager getDatasetPartitionManager() {
        return delegate.getDatasetPartitionManager();
    }

    @Override
    public void sendApplicationMessageToCC(byte[] arg0, DeploymentId arg1) throws Exception {
        delegate.sendApplicationMessageToCC(arg0, arg1);
    }

    @Override
    public ByteBuffer allocateFrame(int bytes) throws HyracksDataException {
        return delegate.allocateFrame(bytes);
    }

    @Override
    public ByteBuffer reallocateFrame(ByteBuffer tobeDeallocate, int newSizeInBytes, boolean copyOldData)
            throws HyracksDataException {
        return delegate.reallocateFrame(tobeDeallocate, newSizeInBytes, copyOldData);
    }

    @Override
    public ExecutorService getExecutorService() {
        return delegate.getExecutorService();
    }

    @Override
    public Object getSharedObject() {
        return delegate.getExecutorService();
    }

    @Override
    public void setSharedObject(Object arg0) {
        delegate.setSharedObject(arg0);
    }
}
