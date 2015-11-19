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

package org.apache.hyracks.imru.api;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.deployment.DeploymentId;
import org.apache.hyracks.api.job.IJobSerializerDeserializer;
import org.apache.hyracks.api.util.JavaSerializationUtils;
import org.apache.hyracks.control.nc.application.NCApplicationContext;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import org.apache.hyracks.imru.util.Rt;

public class IMRUJob2Impl<Model extends Serializable, Data extends Serializable, T extends Serializable> implements
        IIMRUJob2<Model, Data> {
    int fieldCount = 1;
    DeploymentId deploymentId;
    IIMRUJob<Model, Data, T> job;
    private static ExecutorService threadPool = Executors.newCachedThreadPool();

    public IMRUJob2Impl(DeploymentId deploymentId, IIMRUJob<Model, Data, T> job) {
        this.deploymentId = deploymentId;
        this.job = job;
    }

    @Override
    public int getCachedDataFrameSize() {
        return job.getCachedDataFrameSize();
    }

    //    @Override
    //    public Model initModel() {
    //        return job.initModel();
    //    }

    @Override
    public void mapMem(IMRUContext ctx, java.util.Iterator<Data> input, Model model, OutputStream output,
            int cachedDataFrameSize) throws IMRUDataException {
        try {
            T reduceResult;
            T firstResult = job.map(ctx, input, model);
            //            if (!dataInterator.hasNext()) {
            reduceResult = firstResult;
            //            } else {
            //                final ASyncIO<T> io = new ASyncIO<T>();
            //                Future<T> future = threadPool.submit(new Callable<T>() {
            //                    @Override
            //                    public T call() {
            //                        Iterator<T> input = io.getInput();
            //                        try {
            //                            return job.reduce(ctx, input);
            //                        } catch (HyracksDataException e) {
            //                            e.printStackTrace();
            //                        }
            //                        return null;
            //                    }
            //                });
            //                io.add(firstResult);
            //                while (dataInterator.hasNext()) {
            //                    T result = job.map(ctx, dataInterator, model);
            //                    io.add(result);
            //                }
            //                io.close();
            //                reduceResult = future.get();
            //            }
            byte[] objectData = JavaSerializationUtils.serialize(reduceResult);
            output.write(objectData);
            output.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new IMRUDataException(e);
        }
    }

    @Override
    public void map(final IMRUContext ctx, Iterator<ByteBuffer> input, Model model, OutputStream output,
            int cachedDataFrameSize) throws IMRUDataException {
        FrameTupleAccessor accessor = new FrameTupleAccessor(new RecordDescriptor(
                new ISerializerDeserializer[fieldCount]));
        final TupleReader reader = new TupleReader(input, accessor, new ByteBufferInputStream());
        Iterator<Data> dataInterator = new Iterator<Data>() {
            @Override
            public boolean hasNext() {
                return reader.hasNextTuple();
            }

            public Data read() throws Exception {
                int length = reader.readInt();
                if (length == 0) {
                    throw new Exception("map read 0");
                }
                byte[] bs = new byte[length];
                int len = reader.read(bs);
                if (len != length) {
                    throw new Exception("read half");
                }
                NCApplicationContext appContext = (NCApplicationContext) ctx.getJobletContext().getApplicationContext();
                IJobSerializerDeserializer jobSerDe = appContext.getJobSerializerDeserializerContainer()
                        .getJobSerializerDeserializer(deploymentId);
                return (Data) jobSerDe.deserialize(bs);
                //                return (Data) IMRUSerialize.deserialize(bs);
            }

            @Override
            public Data next() {
                if (!hasNext()) {
                    return null;
                }
                try {
                    reader.nextTuple();
                    return read();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public void remove() {
            }
        };
        try {
            T reduceResult;
            T firstResult = job.map(ctx, dataInterator, model);
            //            if (!dataInterator.hasNext()) {
            reduceResult = firstResult;
            //            } else {
            //                final ASyncIO<T> io = new ASyncIO<T>();
            //                Future<T> future = threadPool.submit(new Callable<T>() {
            //                    @Override
            //                    public T call() {
            //                        Iterator<T> input = io.getInput();
            //                        try {
            //                            return job.reduce(ctx, input);
            //                        } catch (HyracksDataException e) {
            //                            e.printStackTrace();
            //                        }
            //                        return null;
            //                    }
            //                });
            //                io.add(firstResult);
            //                while (dataInterator.hasNext()) {
            //                    T result = job.map(ctx, dataInterator, model);
            //                    io.add(result);
            //                }
            //                io.close();
            //                reduceResult = future.get();
            //            }
            byte[] objectData = JavaSerializationUtils.serialize(reduceResult);
            output.write(objectData);
            output.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new IMRUDataException(e);
        }
    }

    @Override
    public void parse(IMRUContext ctx, InputStream input, DataWriter<Data> output) throws IOException {
        job.parse(ctx, input, output);
    }

    @Override
    public void parse(IMRUContext ctx, InputStream in, FrameWriter writer) throws IOException {
        TupleWriter tupleWriter = new TupleWriter(ctx, writer, fieldCount);
        job.parse(ctx, in, new DataWriter<Data>(tupleWriter));
        tupleWriter.close();
    }

    @Override
    public void reduce(final IMRUReduceContext ctx, final Iterator<byte[]> input, OutputStream output)
            throws IMRUDataException {
        Iterator<T> iterator = new Iterator<T>() {
            @Override
            public void remove() {
            }

            @Override
            public boolean hasNext() {
                return input.hasNext();
            }

            @Override
            public T next() {
                byte[] objectData = input.next();
                if (objectData == null) {
                    return null;
                }

                NCApplicationContext appContext = (NCApplicationContext) ctx.getJobletContext().getApplicationContext();
                IJobSerializerDeserializer jobSerDe = appContext.getJobSerializerDeserializerContainer()
                        .getJobSerializerDeserializer(deploymentId);
                try {
                    return (T) jobSerDe.deserialize(objectData);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return null;
            }
        };
        T object = job.reduce(ctx, iterator);
        byte[] objectData;
        try {
            objectData = JavaSerializationUtils.serialize(object);
            output.write(objectData);
            output.close();
        } catch (IOException e) {
            throw new IMRUDataException(e);
        }
    }

    @Override
    public boolean shouldTerminate(Model model) {
        return job.shouldTerminate(model);
    }

    @Override
    public Model update(final IMRUContext ctx, final Iterator<byte[]> input, Model model) throws IMRUDataException {
        Iterator<T> iterator = new Iterator<T>() {
            @Override
            public void remove() {
            }

            @Override
            public boolean hasNext() {
                return input.hasNext();
            }

            @Override
            public T next() {
                byte[] objectData = input.next();
                if (objectData == null) {
                    return null;
                }
                NCApplicationContext appContext = (NCApplicationContext) ctx.getJobletContext().getApplicationContext();
                IJobSerializerDeserializer jobSerDe = appContext.getJobSerializerDeserializerContainer()
                        .getJobSerializerDeserializer(deploymentId);
                try {
                    return (T) jobSerDe.deserialize(objectData);
                } catch (Exception e) {
                    Rt.p("Read reduce result failed len=%,d", objectData.length);
                    e.printStackTrace();
                }
                return null;
            }
        };
        return job.update(ctx, iterator, model);
    }
}