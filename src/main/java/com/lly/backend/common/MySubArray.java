package com.lly.backend.common;

public class MySubArray {
    public byte[] raw;
    public int start;
    public int end;

    public MySubArray(byte[] raw, int start, int end) {
        this.raw = raw;
        this.start = start;
        this.end = end;
    }
}
