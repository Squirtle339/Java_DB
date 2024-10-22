package com.lly.common.utils;

import java.nio.ByteBuffer;

public class Parser {

    public static long getLong(byte[] buf) {
        return ByteBuffer.wrap(buf, 0, 8).getLong();
    }


    public static byte[] long2Byte(long xidCounter) {
        return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(xidCounter).array();
    }

    public static byte[] short2Byte(short shortValue) {
        return ByteBuffer.allocate(Short.SIZE / Byte.SIZE).putShort(shortValue).array();
    }


    public static short offsetByte2Short(byte[] bytes) {
        return ByteBuffer.wrap(bytes, 0, 2).getShort();
    }
}
