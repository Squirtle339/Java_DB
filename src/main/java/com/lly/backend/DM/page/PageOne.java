package com.lly.backend.DM.page;


import com.lly.backend.DM.pageCache.PageCache;
import com.lly.common.utils.RandomUtils;

import java.util.Arrays;

/**
 * 特殊管理第一页
 * 主要用于ValidCheck
 * db启动时给100~107字节处填入一个随机字节，db关闭时将其拷贝到108~115字节
 * 主要用于判断上一次数据库是否正常关闭。如果是异常关闭，就需要执行数据的恢复流程。
 */
public class PageOne {
    private static final int LEN_VC = 8;// ValidCheck校验码的长度
    private static final int OFFSET_VC = 100;// ValidCheck校验码的起始位置

    /**
     * 初始化第一页
     * @return 第一页的byte数组
     */
    public static byte[] InitRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setVcOpen(raw);
        return raw;
    }

    public static void setVcOpen(Page pg) {
        pg.setDirty(true);
        setVcOpen(pg.getData());
    }

    /**
     * 设置ValidCheck校验码，会生成一串随机字节，存储在第一页 100 ~ 107 字节
     * @param raw 第一页的byte数组
     */
    private static void setVcOpen(byte[] raw) {
        System.arraycopy(RandomUtils.randomBytes(LEN_VC), 0, raw, OFFSET_VC, LEN_VC);
    }

    public static void setVcClose(Page pg) {
        pg.setDirty(true);
        setVcClose(pg.getData());
    }

    /**
     * 将ValidCheck校验码拷贝到第一页的108 ~ 115字节
     * @param raw 第一页的byte数组
     */
    private static void setVcClose(byte[] raw) {
        System.arraycopy(raw, OFFSET_VC, raw, OFFSET_VC+LEN_VC, LEN_VC);
    }

    /**
     * 检查ValidCheck校验码是否一致
     * @param pg 第一页
     * @return 是否一致
     */

    public static boolean checkVc(Page pg) {
        return checkVc(pg.getData());
    }


    private static boolean checkVc(byte[] raw) {
        return Arrays.equals(Arrays.copyOfRange(raw, OFFSET_VC, OFFSET_VC+LEN_VC), Arrays.copyOfRange(raw, OFFSET_VC+LEN_VC,OFFSET_VC+2*LEN_VC));
    }
}
