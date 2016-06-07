package edu.uci.ics.hyracks.imru.dataflow;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;

import edu.uci.ics.hyracks.imru.api.IMRUContext;

public class IMRUSerialize {
    public static ExecutorService threadPool = Executors.newCachedThreadPool();
    private static final int BYTES_IN_INT = 4;

    //    public static Object deserialize(byte[] bs) throws Exception {
    //        ByteArrayInputStream in = new ByteArrayInputStream(bs);
    //        ObjectInputStream objIn = new ObjectInputStream(in);
    //        Object obj = objIn.readObject();
    //        objIn.close();
    //        in.close();
    //        return obj;
    //    }

    @SuppressWarnings("unchecked")
    public static byte[] deserializeFromChunks(IMRUContext ctx, List<ByteBuffer> chunks) throws HyracksDataException {
        int size = chunks.get(0).getInt(0);
        byte objectData[] = new byte[size];
        ByteBuffer objectDataByteBuffer = ByteBuffer.wrap(objectData);
        int remaining = size;
        // Handle the first chunk separately, since it contains the object size.
        int length = Math.min(chunks.get(0).array().length - BYTES_IN_INT, remaining);
        objectDataByteBuffer.put(chunks.get(0).array(), BYTES_IN_INT, length);
        remaining -= length;
        // Handle the remaining chunks:
        for (int i = 1; i < chunks.size(); i++) {
            length = Math.min(chunks.get(i).array().length, remaining);
            objectDataByteBuffer.put(chunks.get(i).array(), 0, length);
            remaining -= length;
        }
        return objectData;
    }

    public static void serializeToFrames(IMRUContext ctx, IFrameWriter writer, byte[] objectData)
            throws HyracksDataException {
        ByteBuffer frame = ctx.allocateFrame();
        int position = 0;
        frame.position(0);
        while (position < objectData.length) {
            int length = Math.min(objectData.length - position, ctx.getFrameSize());
            if (position == 0) {
                // The first chunk is a special case, since it begins
                // with an integer containing the length of the
                // serialized object.
                length = Math.min(ctx.getFrameSize() - BYTES_IN_INT, length);
                frame.putInt(objectData.length);
            }
            frame.put(objectData, position, length);
            FrameUtils.flushFrame(frame, writer);
            //            R.p("flush "+ position);
            position += length;
        }
    }
}
