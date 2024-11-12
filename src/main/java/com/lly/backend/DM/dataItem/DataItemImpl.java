package com.lly.backend.DM.dataItem;

import com.lly.backend.DM.DataManagerImpl;
import com.lly.backend.DM.page.Page;
import com.lly.backend.common.MySubArray;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * dataItem 结构如下：
 * [ValidFlag] [DataSize] [Data]
 * ValidFlag 1字节，0为合法，1为非法
 * DataSize  2字节，标识Data的长度
 */
public class DataItemImpl implements DataItem{
    static final int OFFSET_VALID = 0;
    static final int OFFSET_SIZE = 1;
    static final int OFFSET_DATA = 3;

    private MySubArray raw;
    private byte[] oldRaw;

    private Lock rLock;
    private Lock wLock;

    private DataManagerImpl dm;
    private long uid;
    private Page pg;

    public DataItemImpl(MySubArray raw, byte[] oldRaw, Page pg, long uid, DataManagerImpl dm) {
        this.raw = raw;
        this.oldRaw = oldRaw;
        this.dm = dm;
        this.uid = uid;
        this.pg = pg;
        ReadWriteLock lock = new ReentrantReadWriteLock();
        rLock = lock.readLock();
        wLock = lock.writeLock();
    }
    public boolean isValid() {
        return raw.raw[raw.start+OFFSET_VALID] == (byte)0;
    }

    @Override
    public MySubArray data() {
        return new MySubArray(raw.raw, raw.start + OFFSET_DATA, raw.end);
    }

    /*
     * 在修改数据之前调用，包括加锁，设置脏页，保存旧数据
     * 写锁的释放在撤销修改或者提交修改时释放
     */
    @Override
    public void before() {
        wLock.lock();
        pg.setDirty(true);
        System.arraycopy(raw.raw, raw.start, oldRaw, 0, oldRaw.length);
    }

    /*
     * 撤销修改数据时调用，将数据恢复到修改之前的状态
     * 会释放写锁
     */
    @Override
    public void unBefore() {
        System.arraycopy(oldRaw, 0, raw.raw, raw.start, oldRaw.length);
        wLock.unlock();

    }

    /*
     * 在修改数据之后调用，包括记录日志，释放写锁
     */
    @Override
    public void after(long xid) {
        dm.logDataItem(xid, this);
        wLock.unlock();
    }

    /*
     * 释放缓存的一个引用
     */
    @Override
    public void release() {
        dm.releaseDataItem(this);
    }

    @Override
    public void lock() {
        wLock.lock();
    }

    @Override
    public void unlock() {
        wLock.unlock();
    }

    @Override
    public void rLock() {
        rLock.lock();
    }

    @Override
    public void rUnLock() {
        rLock.unlock();
    }

    @Override
    public Page page() {
       return pg;
    }

    @Override
    public long getUid() {
        return uid;
    }

    @Override
    public byte[] getOldRaw() {
        return oldRaw;
    }

    @Override
    public MySubArray getRaw() {
        return raw;
    }
}
