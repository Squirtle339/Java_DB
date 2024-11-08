package com.lly.backend.VM;


import com.google.common.primitives.Bytes;
import com.lly.backend.DM.dataItem.DataItem;
import com.lly.backend.common.MySubArray;
import com.lly.common.utils.Parser;

import java.util.Arrays;

/**
 * VM向上层抽象出entry
 * entry结构：
 * [XMIN] [XMAX] [data]
 */
public class Entry {
    private static final int OFFSET_XMIN = 0;
    private static final int OFFSET_XMAX = OFFSET_XMIN+8;
    private static final int OFFSET_DATA = OFFSET_XMAX+8;

    private long uid;
    //entry存储在dataItem中
    private DataItem dataItem;
    private VersionManager vm;


    public static Entry newEntry(VersionManager vm, DataItem dataItem, long uid) {
        //确保dataItem不为空
        if (dataItem == null) {
            return null;
        }
        Entry entry = new Entry();
        entry.uid = uid;
        entry.dataItem = dataItem;
        entry.vm = vm;
        return entry;
    }

    public static Entry loadEntry(VersionManager vm, long uid) throws Exception {
        //从dm中读取dataItem
        DataItem di = ((VersionManagerImpl)vm).dm.read(uid);
        return newEntry(vm, di, uid);
    }

    //释放entry缓存数据
    public void remove() {
        dataItem.release();
    }

    public void release() {
        ((VersionManagerImpl)vm).releaseEntry(this);
    }


    public static byte[] wrapEntryRaw(long xid, byte[] data) {
        byte[] xmin = Parser.long2Byte(xid);
        byte[] xmax = new byte[8];
        return Bytes.concat(xmin, xmax, data);
    }


    // 以拷贝的形式返回内容
    public byte[] data() {
        dataItem.rLock();
        try {
            MySubArray sa = dataItem.data();
            byte[] data = new byte[sa.end - sa.start - OFFSET_DATA];
            System.arraycopy(sa.raw, sa.start+OFFSET_DATA, data, 0, data.length);
            return data;
        } finally {
            dataItem.rUnLock();
        }
    }

    public long getXmin() {
        dataItem.rLock();
        try {
            MySubArray sa = dataItem.data();
            return Parser.getLong(Arrays.copyOfRange(sa.raw, sa.start+OFFSET_XMIN, sa.start+OFFSET_XMAX));
        } finally {
            dataItem.rUnLock();
        }
    }

    public long getXmax() {
        dataItem.rLock();
        try {
            MySubArray sa = dataItem.data();
            return Parser.getLong(Arrays.copyOfRange(sa.raw, sa.start+OFFSET_XMAX, sa.start+OFFSET_DATA));
        } finally {
            dataItem.rUnLock();
        }
    }

    public void setXmax(long xid) {
        dataItem.before();
        try {
            MySubArray sa = dataItem.data();
            System.arraycopy(Parser.long2Byte(xid), 0, sa.raw, sa.start+OFFSET_XMAX, 8);
        } finally {
            dataItem.after(xid);
        }
    }
    public long getUid() {
        return uid;
    }


}

