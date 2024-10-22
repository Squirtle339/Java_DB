package com.lly.backend.DM.pageCache;

import com.lly.backend.DM.page.Page;
import com.lly.backend.DM.page.PageImpl;
import com.lly.backend.common.AbstractCache;
import com.lly.common.ErrorItem;
import com.lly.common.utils.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
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

    /*
     *根据pageNumber从数据库文件中读取页数据，并封装为Page对象返回
     * @param key 页号
     */
    @Override
    protected Page getForCache(long key) throws Exception {
        int pgno = (int)key;
        long offset=pageOffset(pgno);
        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
        fileLock.lock();
        try {
            fileChannel.position(offset);
            fileChannel.read(buf);
        } catch (Exception e) {
            Error.error(e);
        } finally {
            fileLock.unlock();
        }
        return new PageImpl(pgno, buf.array(), this);
    }

    private long pageOffset(int pgno) {
        return (long) (pgno - 1) * PAGE_SIZE;
    }

    /*
     *释放缓存的Page对象
     * @param obj 页对象
     */
    @Override
    protected void releaseForCache(Page pg) {
        if(pg.isDirty()){
            flushPage(pg);
            pg.setDirty(false);
        }

    }

    /*
     *新建一个空白页，返回页号
     */
    @Override
    public int newPage(byte[] initData) {
        //原子地将当前值加一，并返回新值。
        int pgno = pageNumbers.incrementAndGet();
        Page pg = new PageImpl(pgno, initData, null);
        //将页数据写回到数据库文件中
        flushPage(pg);
        return pgno;
    }


    /*
     *根据页号获取页对象
     */
    @Override
    public Page getPage(int pgno) throws Exception {
        return get((long)pgno);
    }

    /*
     *关闭页缓存
     */
    @Override
    public void close() {
        //在抽象缓存类中会写回所有资源
        super.close();
        try {
            file.close();
            fileChannel.close();
        } catch (IOException e) {
            Error.error(e);
        }

    }

    @Override
    public void release(Page page) {
        //使用抽象缓存类的release方法
        release((long)page.getPageNumber());
    }

    /*
     *根据页号截断数据库文件
     */
    @Override
    public void truncateByPgno(int maxPgno) {
        long size = pageOffset(maxPgno + 1);
        try {
            file.setLength(size);
        } catch (IOException e) {
            Error.error(e);
        }
        pageNumbers.set(maxPgno);

    }

    /*
     *获取页总数
     */
    @Override
    public int getPageNumber() {
        return pageNumbers.intValue();
    }

    /*
     *将页数据写回到数据库文件中
     */
    @Override
    public void flushPage(Page pg) {
        int pgno = pg.getPageNumber();
        long offset = pageOffset(pgno);
        fileLock.lock();
        try {
            ByteBuffer buf = ByteBuffer.wrap(pg.getData());
            fileChannel.position(offset);
            fileChannel.write(buf);
            fileChannel.force(false);
        } catch(IOException e) {
            Error.error(e);
        } finally {
            fileLock.unlock();
        }
    }
}
