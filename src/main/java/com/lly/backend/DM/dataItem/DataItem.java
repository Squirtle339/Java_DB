package com.lly.backend.DM.dataItem;

import com.lly.backend.DM.page.Page;
import com.lly.backend.common.MySubArray;

public interface DataItem {
    MySubArray data();

    void before();
    void unBefore();
    void after(long xid);
    void release();

    void lock();
    void unlock();
    void rLock();
    void rUnLock();

    Page page();
    long getUid();
    byte[] getOldRaw();
    MySubArray getRaw();
}
