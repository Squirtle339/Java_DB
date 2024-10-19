package com.lly.common.utils;

import java.nio.ByteBuffer;

public class Parser {

    public static long getLong(byte[] buf) {
        return ByteBuffer.wrap(buf, 0, 8).getLong();
    }


    public static byte[] long2Byte(long xidCounter) {
        return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(xidCounter).array();
    }
}
