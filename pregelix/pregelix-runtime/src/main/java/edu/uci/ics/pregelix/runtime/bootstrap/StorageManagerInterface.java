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
package edu.uci.ics.pregelix.runtime.bootstrap;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.storage.common.IStorageManagerInterface;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.file.IFileMapProvider;
import org.apache.hyracks.storage.common.file.ILocalResourceRepository;
import org.apache.hyracks.storage.common.file.ResourceIdFactory;
import edu.uci.ics.pregelix.dataflow.context.RuntimeContext;

public class StorageManagerInterface implements IStorageManagerInterface {
    private static final long serialVersionUID = 1L;

    public static final StorageManagerInterface INSTANCE = new StorageManagerInterface();

    private StorageManagerInterface() {
    }

    @Override
    public IBufferCache getBufferCache(IHyracksTaskContext ctx) {
        return RuntimeContext.get(ctx).getBufferCache();
    }

    @Override
    public IFileMapProvider getFileMapProvider(IHyracksTaskContext ctx) {
        return RuntimeContext.get(ctx).getFileMapManager();
    }

    @Override
    public ILocalResourceRepository getLocalResourceRepository(IHyracksTaskContext ctx) {
        return RuntimeContext.get(ctx).getLocalResourceRepository();
    }

    @Override
    public ResourceIdFactory getResourceIdFactory(IHyracksTaskContext ctx) {
        return RuntimeContext.get(ctx).getResourceIdFactory();
    }
}