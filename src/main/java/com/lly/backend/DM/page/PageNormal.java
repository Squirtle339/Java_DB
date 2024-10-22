package com.lly.backend.DM.page;


import com.lly.backend.DM.pageCache.PageCache;
import com.lly.common.utils.Parser;

import java.util.Arrays;

/**
 * PageNormal管理普通页
 * 普通页结构
 * [FreeSpaceOffset] [Data]
 * FreeSpaceOffset: 2字节 空闲位置开始偏移
 */
public class PageNormal {
    private static final short OFFSET_FREE = 0; // 空闲位置开始的偏移
    private static final short OFFSET_DATA = 2; // 数据开始的偏移
    public static final int MAX_FREE_SPACE = PageCache.PAGE_SIZE - OFFSET_DATA;// 最大的空闲空间

    /*
     * 初始化一个普通页
     * @return 一个普通页的byte数组
     */
    public static byte[] initRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setFSO(raw, OFFSET_DATA);
        return raw;
    }

    /*
     * 设置FreeSpaceOffset
     */
    private static void setFSO(byte[] raw, short ofData) {
        System.arraycopy(Parser.short2Byte(ofData), 0, raw, OFFSET_FREE, OFFSET_DATA);
    }

    // 获取pg的FSO
    public static short getFSO(Page pg) {
        return getFSO(pg.getData());
    }

    private static short getFSO(byte[] raw) {
        return Parser.offsetByte2Short(Arrays.copyOfRange(raw, 0, 2));
    }

    // 获取页面的空闲空间大小
    public static int getFreeSpace(Page pg) {
        return PageCache.PAGE_SIZE - (int)getFSO(pg.getData());
    }


    /*
     * 将raw插入pg中，返回插入位置
     */
    public static short insert(Page pg, byte[] raw) {
        pg.setDirty(true);
        short offset = getFSO(pg.getData());
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
        setFSO(pg.getData(), (short)(offset + raw.length));
        return offset;
    }

    /*
     * 将raw插入pg中的offset位置，并将pg的offset设置为较大的offset
     * 用于在数据库崩溃后重新打开时，恢复例程直接插入数据以及修改数据使用
     */
    public static void recoverInsert(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);

        short rawFSO = getFSO(pg.getData());
        if(rawFSO < offset + raw.length) {
            setFSO(pg.getData(), (short)(offset+raw.length));
        }
    }

    /*
     * 将raw插入pg中的offset位置，不更新update
     * 用于在数据库崩溃后重新打开时，恢复例程直接修改数据使用
     */
    public static void recoverUpdate(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
    }


}
