package edu.uci.ics.hyracks.imru.runtime.bootstrap;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Random;

import org.apache.hyracks.api.util.JavaSerializationUtils;

import edu.uci.ics.hyracks.imru.util.Rt;

public class IMRUConnection implements Serializable {
    String host;
    int port;
    private final String downloadKey;

    public IMRUConnection(String host, int port) {
        this.host = host;
        this.port = port;
        byte[] bs = new byte[128];
        Random random = new Random();
        for (int i = 0; i < bs.length; i++)
            bs[i] = (byte) ('a' + random.nextInt(26));
        downloadKey = new String(bs);
    }

    public void uploadModel(String name, Serializable model) throws IOException {
        uploadData(name, JavaSerializationUtils.serialize(model));
    }

    public Serializable downloadModel(String name) throws IOException, ClassNotFoundException {
        byte[] bs = downloadData(name);
        return (Serializable) JavaSerializationUtils.deserialize(bs, this.getClass().getClassLoader());
    }

    public void uploadData(String name, byte[] model) throws IOException {
        URL url2 = new URL("http://" + host + ":" + port + "/put?key=" + URLEncoder.encode(downloadKey, "UTF-8")
                + "&name=" + URLEncoder.encode(name, "UTF-8"));
        HttpURLConnection connection = (HttpURLConnection) url2.openConnection();
        connection.setDoOutput(true);
        connection.setInstanceFollowRedirects(false);
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", "application/octet-stream");
        connection.connect();
        OutputStream out = connection.getOutputStream();
        out.write(model);
        out.close();
        byte[] bs = Rt.read(connection.getInputStream());
        String result = new String(bs).trim();
        if (!"ok".equals(result))
            throw new IOException(result);
    }

    public byte[] downloadData(String name) throws IOException {
        URL url2 = new URL("http://" + host + ":" + port + "/get?key=" + URLEncoder.encode(downloadKey, "UTF-8")
                + "&name=" + URLEncoder.encode(name, "UTF-8"));
        HttpURLConnection connection = (HttpURLConnection) url2.openConnection();
        connection.setInstanceFollowRedirects(false);
        connection.setRequestMethod("GET");
        connection.connect();
        return Rt.read(connection.getInputStream());
    }

    public void setStatus(String job, String status) throws IOException {
        URL url2 = new URL("http://" + host + ":" + port + "/setStatus?jobName=" + URLEncoder.encode(job, "UTF-8")
                + "&status=" + URLEncoder.encode(status, "UTF-8"));
        HttpURLConnection connection = (HttpURLConnection) url2.openConnection();
        connection.setInstanceFollowRedirects(false);
        connection.setRequestMethod("GET");
        connection.connect();
        Rt.read(connection.getInputStream());
    }

    public String getStatus(String job) throws IOException {
        URL url2 = new URL("http://" + host + ":" + port + "/getStatus?jobName=" + URLEncoder.encode(job, "UTF-8"));
        HttpURLConnection connection = (HttpURLConnection) url2.openConnection();
        connection.setInstanceFollowRedirects(false);
        connection.setRequestMethod("GET");
        connection.connect();
        return new String(Rt.read(connection.getInputStream()));
    }

    public void finishJob(String job) throws IOException {
        URL url2 = new URL("http://" + host + ":" + port + "/finishJob?jobName=" + URLEncoder.encode(job, "UTF-8"));
        HttpURLConnection connection = (HttpURLConnection) url2.openConnection();
        connection.setInstanceFollowRedirects(false);
        connection.setRequestMethod("GET");
        connection.connect();
        Rt.read(connection.getInputStream());
    }
}
