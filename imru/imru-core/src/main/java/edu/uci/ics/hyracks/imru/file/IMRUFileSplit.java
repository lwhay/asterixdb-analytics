package edu.uci.ics.hyracks.imru.file;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.Hashtable;
import java.util.List;
import java.util.Vector;

public class IMRUFileSplit implements Serializable {
    String path;
    //    HDFSSplit split;
    Hashtable<String, String> params;

    public IMRUFileSplit(String path) {
        this.path = path;
    }

    //    public IMRUFileSplit(HDFSSplit split) {
    //        this.split = split;
    //    }

    public boolean isOnHDFS() {
        return path == null;
    }

    public String getPath() {
        String path = this.path;
        int t = path.indexOf('?');
        if (t > 0)
            path = path.substring(0, t);
        t = path.indexOf(':');
        if (t > 0)
            path = path.substring(t + 1);
        return path;
    }

    public String getParameter(String key) {
        if (params == null) {
            params = new Hashtable<String, String>();
            int t = path.indexOf('?');
            if (t < 0)
                return null;
            for (String s : path.substring(t + 1).split("&")) {
                String[] kv = s.split("=", 2);
                params.put(kv[0], kv[1]);
            }
        }
        return params.get(key);
    }

    public boolean isDirectory() throws IOException {
        //        if (path != null)
        return new File(path).isDirectory();
        //        else
        //            return split.isDirectory();
    }

    //    public String[] getLocations() throws IOException {
    //        return split.getLocations();
    //    }

    public static List<IMRUFileSplit> get(String[] paths) {
        Vector<IMRUFileSplit> list = new Vector<IMRUFileSplit>(paths.length);
        for (String path : paths)
            list.add(new IMRUFileSplit(path));
        return list;
    }

    public IMRUFileSplit(DataInput input) throws IOException {
        boolean hdfs = input.readBoolean();
        int n = input.readInt();
        char[] cs = new char[n];
        for (int i = 0; i < n; i++)
            cs[i] = input.readChar();
        //        if (hdfs) {
        //            split = new HDFSSplit(new String(cs));
        //        } else {
        path = new String(cs);
        //        }
    }

    public void write(DataOutput output) throws IOException {
        //        if (path != null) {
        output.writeBoolean(false); //file system
        output.writeInt(path.length());
        output.writeChars(path);
        //        } else {
        //            output.writeBoolean(true); //HDFS
        //            output.writeInt(split.path.length());
        //            output.writeChars(split.path);
        //        }
    }

    public InputStream getInputStream() throws IOException {
        //        if (path != null) {
        String path = this.path;
        if (path.indexOf(':') > 0)
            path = path.substring(path.indexOf(':') + 1);
        return new FileInputStream(path);
        //        } else {
        //            return split.getInputStream();
        //        }
    }

    public BufferedReader getReader() throws IOException {
        return new BufferedReader(new InputStreamReader(getInputStream()));
    }

    @Override
    public String toString() {
        return path;
        //        return path != null ? path : split.toString();
    }
}
