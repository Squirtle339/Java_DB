package com.lly.common.utils;

import com.google.common.primitives.Bytes;
import com.lly.backend.TBM.Result.ParseStringRes;

import java.nio.ByteBuffer;
import java.util.Arrays;

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

    public static byte[] int2Byte(int i) {
        return ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(i).array();
    }

    public static int getInt(byte[] buf) {
        return ByteBuffer.wrap(buf, 0, 4).getInt();
    }

    public static short getShort(byte[] buf) {
        return ByteBuffer.wrap(buf, 0, 2).getShort();
    }

    public static ParseStringRes parseString(byte[] raw) {
        int length = getInt(Arrays.copyOf(raw, 4));
        String str = new String(Arrays.copyOfRange(raw, 4, 4 + length));
        return new ParseStringRes(str, length + 4);
    }

    /**
     * 把字符串转换自定义的字符串字节格式
     * @return [StringLength][StringData]
     */
    public static byte[] string2Byte(String str) {
        byte[] l = int2Byte(str.length());
        return Bytes.concat(l, str.getBytes());
    }

    /**
     * 字符串类型的转换为索引的key
     * @param str
     * @return
     */
    public static long str2key(String str) {
        long seed = 13331;
        long res = 0;
        for(byte b : str.getBytes()) {
            res = res * seed + (long)b;
        }
        return res;
    }


}
