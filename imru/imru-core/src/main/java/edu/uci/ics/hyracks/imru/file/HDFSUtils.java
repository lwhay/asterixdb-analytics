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

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

/**
 * Utility functions for working with HDFS.
 *
 * @author Josh Rosen
 */
public class HDFSUtils {
    /**
     * Open a file in HDFS for reading, performing automatic
     * decompression as necessary.
     *
     * @param dfs
     *            The HDFS file system object.
     * @param conf
     *            The HDFS configuration.
     * @param path
     *            The path to the file.
     * @return An InputStream for reading the file.
     * @throws IOException
     */
    public static InputStream open(FileSystem dfs, Configuration conf, Path path) throws IOException {
        FSDataInputStream fin = dfs.open(path);
        CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(conf);
        final CompressionCodec codec = compressionCodecs.getCodec(path);
        if (codec != null) {
            return codec.createInputStream(fin);
        } else {
            return fin;
        }
    }
}
