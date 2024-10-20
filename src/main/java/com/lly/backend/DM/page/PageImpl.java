package com.lly.backend.DM.page;

import sun.jvm.hotspot.debugger.PageCache;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageImpl implements Page{
    private int pageNumber;
    private byte[] data;
    private boolean isdirty;    //在缓存驱逐的时候，脏页面需要被写回磁盘
    private Lock lock;

    private PageCache pageCache;


    public PageImpl(int pageNumber, byte[] data, PageCache pageCache) {
        this.pageNumber = pageNumber;
        this.data = data;
        this.pageCache = pageCache;
        lock = new ReentrantLock();
    }


    @Override
    public void lock() {

    }

    @Override
    public void unlock() {

    }

    @Override
    public void release() {

    }

    @Override
    public void setDirty(boolean dirty) {

    }

    @Override
    public boolean isDirty() {
        return false;
    }

    @Override
    public int getPageNumber() {
        return 0;
    }

    @Override
    public byte[] getData() {
        return new byte[0];
    }
}
