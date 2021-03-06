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
import org.apache.hyracks.storage.am.common.api.IIndexLifecycleManager;
import org.apache.hyracks.storage.am.common.api.IIndexLifecycleManagerProvider;
import edu.uci.ics.pregelix.dataflow.context.RuntimeContext;

public class IndexLifeCycleManagerProvider implements IIndexLifecycleManagerProvider {

    private static final long serialVersionUID = 1L;

    public static final IIndexLifecycleManagerProvider INSTANCE = new IndexLifeCycleManagerProvider();

    private IndexLifeCycleManagerProvider() {
    }

    @Override
    public IIndexLifecycleManager getLifecycleManager(IHyracksTaskContext ctx) {
        return RuntimeContext.get(ctx).getIndexLifecycleManager();
    }

}
