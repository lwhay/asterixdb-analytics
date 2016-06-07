package edu.uci.ics.hyracks.imru.dataflow;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

public class IMRUDebugger {
    public static boolean debug = false;
    static InetAddress address;
    static DatagramSocket serverSocket;

    public static void sendDebugInfo(String s) {
        if (!debug)
            return;
        try {
            if (serverSocket == null) {
                String host = "192.168.56.101";
                address = InetAddress.getByName(host);
                serverSocket = new DatagramSocket();
            }
            byte[] bs = s.getBytes();
            DatagramPacket sendPacket = new DatagramPacket(bs, bs.length, address, 6667);
            serverSocket.send(sendPacket);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
