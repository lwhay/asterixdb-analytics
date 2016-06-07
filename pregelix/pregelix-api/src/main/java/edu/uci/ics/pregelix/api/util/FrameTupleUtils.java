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

package edu.uci.ics.pregelix.api.util;

import org.apache.hyracks.api.comm.IFrameFieldAppender;
import org.apache.hyracks.api.comm.IFrameTupleAppender;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;

import edu.uci.ics.pregelix.api.io.Pointable;

public class FrameTupleUtils {

    public static void flushTuple(IFrameFieldAppender appender, ArrayTupleBuilder tb, IFrameWriter writer)
            throws HyracksDataException {
        FrameUtils.appendToWriter(writer, (IFrameTupleAppender) appender, tb.getFieldEndOffsets(), tb.getByteArray(),
                0, tb.getSize());
    }

    public static void flushPointableKeyValueTuple(IFrameFieldAppender appender, IFrameWriter writer, Pointable key,
            Pointable value) throws HyracksDataException {
        if (!flushPointableKeyValueTupleInternal(appender, key, value)) {
            appender.flush(writer);
            //appender.flush(writer, true);
            if (!flushPointableKeyValueTupleInternal(appender, key, value)) {
                throw new IllegalStateException();
            }
        }
    }

    private static boolean flushPointableKeyValueTupleInternal(IFrameFieldAppender appender, Pointable key,
            Pointable value) throws HyracksDataException {
        if (!appender.appendField(key.getByteArray(), key.getStartOffset(), key.getLength())) {
            return false;
        }
        if (!appender.appendField(value.getByteArray(), value.getStartOffset(), value.getLength())) {
            return false;
        }
        return true;
    }

    public static void flushTuplesFinal(IFrameFieldAppender appender, IFrameWriter writer) throws HyracksDataException {
        if (appender.getTupleCount() > 0) {
            appender.flush(writer);
            //appender.flush(writer, true);
        }
    }
}
