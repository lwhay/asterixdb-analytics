/*
 * Copyright 20092013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.external.connector.asterixdb.dataflow.helper;

import java.util.List;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.dataflow.IIndexOperatorDescriptor;
import org.apache.hyracks.storage.am.lsm.btree.dataflow.LSMBTreeDataflowHelper;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerProvider;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.common.file.ILocalResourceFactory;

public class AsterixDBLSMBTreeDataflowHelper extends LSMBTreeDataflowHelper {

    public AsterixDBLSMBTreeDataflowHelper(IIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx, int partition,
            List<IVirtualBufferCache> virtualBufferCaches, double bloomFilterFalsePositiveRate,
            ILSMMergePolicy mergePolicy, ILSMOperationTrackerProvider opTrackerFactory,
            ILSMIOOperationScheduler ioScheduler, ILSMIOOperationCallbackFactory ioOpCallbackFactory,
            boolean needKeyDupCheck, ITypeTraits[] filterTypeTraits, IBinaryComparatorFactory[] filterCmpFactories,
            int[] btreeFields, int[] filterFields, boolean durable) {
        super(opDesc, ctx, partition, virtualBufferCaches, bloomFilterFalsePositiveRate, mergePolicy, opTrackerFactory,
                ioScheduler, ioOpCallbackFactory, needKeyDupCheck, filterTypeTraits, filterCmpFactories, btreeFields,
                filterFields, durable);
    }

    @Override
    public void open() throws HyracksDataException {
        synchronized (lcManager) {
            long resourceID = getResourceID();
            if (resourceID == 1) {
                resourceID = addLocalResource();
            }

            index = lcManager.getIndex(resourcePath);
            if (index == null) {
                index = createIndexInstance();
                lcManager.register(resourcePath, index);
            }
            lcManager.open(resourcePath);
        }
    }

    @Override
    public void close() throws HyracksDataException {
        synchronized (lcManager) {
            // Closes the resource first.
            lcManager.close(resourcePath);

            // Cleanup things associated with the resource.
            index = lcManager.getIndex(resourcePath);
            if (index != null) {
                // Unregister will deactivate the index.
                lcManager.unregister(resourcePath);
            }
        }
    }

    private long addLocalResource() throws HyracksDataException {
        long resourceID = resourceIdFactory.createId();
        ILocalResourceFactory localResourceFactory = opDesc.getLocalResourceFactoryProvider().getLocalResourceFactory();
        localResourceRepository.insert(localResourceFactory.createLocalResource(resourceID, file.getFile().getPath(),
                partition, resourcePath));
        /*localResourceRepository
                .insert(localResourceFactory.createLocalResource(resourceID, file.getFile().getPath(), partition));*/
        return resourceID;
    }
}
