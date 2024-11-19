package com.lly.client;

import com.lly.backend.transport.Package;
import com.lly.backend.transport.Packager;

public class Client {

    private RoundTripper rt;

    public Client(Packager packager) {
        this.rt = new RoundTripper(packager);
    }

    public byte[] execute(byte[] stat) throws Exception {
        Package pkg = new Package(stat, null);
        //发送请求并接收响应
        Package resPkg = rt.roundTrip(pkg);
        if(resPkg.getErr() != null) {
            throw resPkg.getErr();
        }
        return resPkg.getData();
    }

    public void close() {
        try{
            rt.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
