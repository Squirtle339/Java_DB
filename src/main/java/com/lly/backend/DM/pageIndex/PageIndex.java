package com.lly.backend.DM.pageIndex;


import com.lly.backend.DM.pageCache.PageCache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * PageIndex,缓存了每一页的空闲空间大小,
 * 用于在上层模块进行插入操作时，能够快速找到一个合适空间的页面，而无需从磁盘或者缓存中检查每一个页面的信息。
 */
public class PageIndex {
    // 一个页分为40个区间进行存储
    private static final int INTERVALS_NO = 40;
    // 每个区间的大小
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;
    private Lock lock;
    private List<PageInfo>[] lists;

    public PageIndex() {
        lock = new ReentrantLock();
        lists = new List[INTERVALS_NO+1];
        for (int i = 0; i < INTERVALS_NO+1; i ++) {
            lists[i] = new ArrayList<>();
        }
    }

    /*
     * 添加一个页面的信息到PageIndex中
     *
     * @param pgno 页面号
     * @param freeSpace 空闲空间大小
     */
    public void add(int pgno, int freeSpace) {
        lock.lock();
        try {
            // 计算空闲区间的数量
            int number = freeSpace / THRESHOLD;
            lists[number].add(new PageInfo(pgno, freeSpace));
        } finally {
            lock.unlock();
        }
    }

    /*
     * 选择一个合适的页面
     * 选择一个页进行写时会在lists删除，不允许并发写，后续需要重新add
     * @param spaceSize 空间大小
     * @return PageInfo
     */
    public PageInfo select(int spaceSize) {
        lock.lock();
        try {
            // 计算空间大小所需的区间数量
            int number = spaceSize / THRESHOLD;
            //不优雅的向上取整
            if(number < INTERVALS_NO) number ++;
            while(number <= INTERVALS_NO) {
                if(lists[number].isEmpty()) {
                    number ++;
                    continue;
                }
                return lists[number].remove(0);
            }
            return null;
        } finally {
            lock.unlock();
        }
    }
}
