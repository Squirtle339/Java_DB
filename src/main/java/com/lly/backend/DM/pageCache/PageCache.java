package com.lly.backend.DM.pageCache;

import com.lly.backend.DM.page.Page;

public interface PageCache {
    // 8KB
    public static final int PAGE_SIZE = 1 << 13;

    int newPage(byte[] initData);
    Page getPage(int pgno) throws Exception;
    void close();
    void release(Page page);
    void truncateByPgno(int maxPgno);
    int getPageNumber();
    void flushPage(Page pg);
}
