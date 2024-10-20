package com.lly.backend.DM.pageCache;

import com.lly.backend.DM.page.Page;
import com.lly.backend.common.AbstractCache;
import com.lly.common.ErrorItem;
import com.lly.common.utils.Error;

import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageCacheImpl extends AbstractCache<Page> implements PageCache {

    private static final int MEM_MIN_LIM = 10;
    public static final String DB_SUFFIX = ".db";

    private RandomAccessFile file;
    private FileChannel fileChannel;
    private Lock fileLock;

    //在多线程环境下，无需额外的同步措施，即可保证操作的线程安全性
    private AtomicInteger pageNumbers;

    PageCacheImpl(RandomAccessFile file, FileChannel fileChannel, int maxResource) {
        super(maxResource);
        //缓存数最小限制
        if(maxResource<MEM_MIN_LIM){
            Error.error(ErrorItem.MemTooSmallException);
        }
        long length = 0;
        try {
            length = file.length();
        } catch (Exception e) {
            Error.error(e);
        }
        this.file = file;
        this.fileChannel = fileChannel;
        this.fileLock = new ReentrantLock();
        this.pageNumbers=new AtomicInteger((int)length/PAGE_SIZE);
    }

    @Override
    protected Page getForCache(long key) throws Exception {
        return null;
    }

    @Override
    protected void releaseForCache(Page obj) {

    }


    @Override
    public int newPage(byte[] initData) {
        return 0;
    }

    @Override
    public Page getPage(int pgno) throws Exception {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public void release(Page page) {

    }

    @Override
    public void truncateByBgno(int maxPgno) {

    }

    @Override
    public int getPageNumber() {
        return 0;
    }

    @Override
    public void flushPage(Page pg) {

    }
}
