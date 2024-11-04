package com.lly.backend.DM.pageCache;

import com.lly.backend.DM.page.Page;
import com.lly.common.ErrorItem;
import com.lly.common.utils.Error;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

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

    public static PageCacheImpl create(String path, long memory) {
        File f = new File(path+PageCacheImpl.DB_SUFFIX);
        try {
            if(!f.createNewFile()) {
                Error.error(ErrorItem.FileExistsException);
            }
        } catch (Exception e) {
            Error.error(e);
        }
        if(!f.canRead() || !f.canWrite()) {
           Error.error(ErrorItem.FileCannotRWException);
        }
        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Error.error(e);
        }
        return new PageCacheImpl(raf, fc, (int)memory/PAGE_SIZE);
    }

    public static PageCacheImpl open(String path, long memory) {
        File f = new File(path+PageCacheImpl.DB_SUFFIX);
        if(!f.exists()) {
            Error.error(ErrorItem.FileNotExistsException);
        }
        if(!f.canRead() || !f.canWrite()) {
            Error.error(ErrorItem.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Error.error(e);
        }
        return new PageCacheImpl(raf, fc, (int)memory/PAGE_SIZE);
    }
}
