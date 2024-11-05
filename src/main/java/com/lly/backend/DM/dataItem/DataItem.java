package com.lly.backend.DM.dataItem;

import com.google.common.primitives.Bytes;
import com.lly.backend.DM.DataManagerImpl;
import com.lly.backend.DM.page.Page;
import com.lly.backend.common.MySubArray;
import com.lly.common.utils.Parser;
import com.lly.common.utils.Types;

import java.util.Arrays;

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

    // 从页面的offset处解析处dataItem
    public  static DataItem parseDataItem(Page page, short offset, DataManagerImpl dm) {
        byte[] raw = page.getData();
        short size = Parser.getShort(Arrays.copyOfRange(raw, offset+DataItemImpl.OFFSET_SIZE, offset+DataItemImpl.OFFSET_DATA));
        short length = (short)(size + DataItemImpl.OFFSET_DATA);
        long uid = Types.addressToUid(page.getPageNumber(), offset);
        return new DataItemImpl(new MySubArray(raw, offset, offset+length), new byte[length], page, uid, dm);
    }

    //封装数据为DataItem格式
    public static byte[] wrapDataItemRaw(byte[] raw) {
        byte[] valid = new byte[1];
        byte[] size = Parser.short2Byte((short)raw.length);
        return Bytes.concat(valid, size, raw);
    }

    static void setDataItemRawInvalid(byte[] raw) {
        raw[DataItemImpl.OFFSET_VALID] = (byte)1;
    }
}
