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
package edu.uci.ics.hyracks.imru.runtime.bootstrap;

import java.util.logging.Logger;

import org.apache.hyracks.api.application.INCApplicationContext;
import org.apache.hyracks.api.application.INCApplicationEntryPoint;

/**
 * Bootstrap for creating the IMRU application context on node controllers.
 */
public class IMRUNCBootstrapImpl implements INCApplicationEntryPoint {
    private static final Logger LOGGER = Logger.getLogger(IMRUNCBootstrapImpl.class.getName());
    private INCApplicationContext appCtx;

    @Override
    public void start(INCApplicationContext appCtx, String[] arg1) throws Exception {
        this.appCtx = appCtx;
        LOGGER.info("Starting NC Bootstrap");
        IMRURuntimeContext rCtx = new IMRURuntimeContext(appCtx);
        appCtx.setApplicationObject(rCtx);
        LOGGER.info("Initialized RuntimeContext: " + rCtx);
    }

    @Override
    public void notifyStartupComplete() throws Exception {
        // TODO Auto-generated method stub

    }

    @Override
    public void stop() throws Exception {
        LOGGER.info("Stopping IMRU NC Bootstrap");
        IMRURuntimeContext rCtx = (IMRURuntimeContext) appCtx.getApplicationObject();
        rCtx.close();
    }
}