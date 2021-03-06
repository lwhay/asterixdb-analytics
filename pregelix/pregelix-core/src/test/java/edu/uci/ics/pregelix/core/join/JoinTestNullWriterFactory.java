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
package edu.uci.ics.pregelix.core.join;

import java.io.DataOutput;

import org.apache.hyracks.api.dataflow.value.INullWriter;
import org.apache.hyracks.api.dataflow.value.INullWriterFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;

public class JoinTestNullWriterFactory implements INullWriterFactory {
    private static final long serialVersionUID = 1L;
    public static INullWriterFactory INSTANCE = new JoinTestNullWriterFactory();

    @Override
    public INullWriter createNullWriter() {
        return new INullWriter() {
            UTF8StringSerializerDeserializer serde = new UTF8StringSerializerDeserializer();

            @Override
            public void writeNull(DataOutput out) throws HyracksDataException {
                serde.serialize("NULL", out);
            }

        };
    }

}
