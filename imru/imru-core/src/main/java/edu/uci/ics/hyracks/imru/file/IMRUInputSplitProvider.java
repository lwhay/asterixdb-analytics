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

package edu.uci.ics.hyracks.imru.file;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.mapreduce.InputSplit;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.input.FileSplit;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * Provides a list of InputSplits.
 * <p>
 * This class is necessary because InputSplit is not Serializable. When InputSplit is serialized and deserialized, its locations are lost and getLocations() will return an empty array (this is also the case in Hadoop).
 * <p>
 * This class is thread-safe.
 * 
 * @author Josh Rosen
 */
public class IMRUInputSplitProvider implements Serializable {
    private static final long serialVersionUID = 1L;

    /** The serialized InputSplits. */
    private byte[] data;
    private final int numSplits;
    private transient List<IMRUFileSplit> splits = null;

    /**
     * Construct a new InputSplitProvider.
     * 
     * @param inputPaths
     *            A comma-separated list of input paths.
     * @param conf
     *            The configuration for connecting to HDFS.
     * @throws InterruptedException
     */
    public IMRUInputSplitProvider(String inputPaths, ConfigurationFactory confFactory) throws InterruptedException {
        try {
            String[] ss = inputPaths.split(",");
            //            if (confFactory == null || !confFactory.useHDFS()) {
            splits = IMRUFileSplit.get(ss);
            //            } else {
            //                splits = HDFSSplit.get(confFactory, ss);
            //            }
            numSplits = splits.size();
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutput output = new DataOutputStream(baos);
            for (IMRUFileSplit split : splits)
                split.write(output);
            data = baos.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public static IMRUFileSplit[] getInputSplits(String inputPaths) throws HyracksDataException, InterruptedException {
        return getInputSplits(inputPaths, null);
    }

    public static IMRUFileSplit[] getInputSplits(String inputPaths, ConfigurationFactory confFactory)
            throws HyracksDataException, InterruptedException {
        IMRUInputSplitProvider inputSplitProvider = new IMRUInputSplitProvider(inputPaths, confFactory);
        List<IMRUFileSplit> inputSplits = inputSplitProvider.getInputSplits();
        return inputSplits.toArray(new IMRUFileSplit[inputSplits.size()]);
    }

    /**
     * @return The list of InputSplits.
     */
    public List<IMRUFileSplit> getInputSplits() {
        return splits;
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        // Reconstruct the splits array after deserialization.
        splits = new ArrayList<IMRUFileSplit>(numSplits);
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        DataInput input = new DataInputStream(bais);
        for (int i = 0; i < numSplits; i++) {
            IMRUFileSplit split = new IMRUFileSplit(input);
            splits.add(split);
        }
    }
}
