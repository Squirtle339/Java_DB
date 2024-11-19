package com.lly.backend.transport;

import java.io.IOException;

/**
 * 封装传输层和编码解码器，为server提供简单的收发接口
 */
public class Packager {
    private Transporter transpoter;
    private Encoder encoder;

    public Packager(Transporter transporter, Encoder encoder) {
        this.transpoter = transporter;
        this.encoder = encoder;
    }

    public Package receive() throws Exception {
        byte[] data = transpoter.receive();
        return encoder.decode(data);
    }

    public void send(Package pkg) throws IOException {
        byte[] data = encoder.encode(pkg);
        transpoter.send(data);
    }

    public void close() throws IOException {
        transpoter.close();
    }
}
