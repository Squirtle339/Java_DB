package com.lly.backend.DM;

import com.lly.backend.DM.dataItem.DataItem;
import com.lly.backend.DM.dataItem.DataItemImpl;
import com.lly.backend.DM.logger.Logger;
import com.lly.backend.DM.page.Page;
import com.lly.backend.DM.page.PageNormal;
import com.lly.backend.DM.page.PageOne;
import com.lly.backend.DM.pageCache.PageCache;
import com.lly.backend.DM.pageIndex.PageIndex;
import com.lly.backend.DM.pageIndex.PageInfo;
import com.lly.backend.TM.TransactionManager;
import com.lly.backend.common.AbstractCache;
import com.lly.common.ErrorItem;
import com.lly.common.utils.Error;
import com.lly.common.utils.Types;

public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager{
    TransactionManager tm;
    Logger logger;
    PageCache pc;

    PageIndex pageIndex;
    Page pageOne;

    public DataManagerImpl(PageCache pc, Logger lg, TransactionManager tm) {
        super(0);
        this.pc = pc;
        this.logger = lg;
        this.tm = tm;
        this.pageIndex = new PageIndex();
    }



    @Override
    protected DataItem getForCache(long uid) throws Exception {
        short offset = (short)(uid & ((1L << 16) - 1));
        uid >>>= 32;
        int pgno = (int)(uid & ((1L << 32) - 1));

        Page page = pc.getPage(pgno);
        return DataItem.parseDataItem(page, offset, this);
    }

    @Override
    protected void releaseForCache(DataItem dataItem) {
        dataItem.page().release();
    }


    @Override
    public DataItem read(long uid) throws Exception {
        DataItemImpl dataItem = (DataItemImpl)super.get(uid);
        if(!dataItem.isValid()) {
           dataItem.release();
           return null;
        }
        return dataItem;
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        byte[] raw = DataItem.wrapDataItemRaw(data);
        if(raw.length > PageNormal.MAX_FREE_SPACE) {
            throw ErrorItem.DataTooLargeException;
        }
        // 尝试获取可用页
        PageInfo pi = null;
        for(int i = 0; i < 5; i ++) {
            pi = pageIndex.select(raw.length);
            if (pi != null) {
                break;
            }
            //没有可用页则新建一个页
            else {
                int newPgno = pc.newPage(PageNormal.initRaw());
                pageIndex.add(newPgno, PageNormal.MAX_FREE_SPACE);
            }
        }
        if(pi == null) {
            throw ErrorItem.DatabaseBusyException;
        }

        Page pg = null;
        int freeSpace = 0;
        try {
            pg = pc.getPage(pi.pgno);
            byte[] log = Recover.insertLog(xid, pg, raw);
            logger.log(log);

            short offset = PageNormal.insert(pg, raw);

            pg.release();
            return Types.addressToUid(pi.pgno, offset);

        } finally {
            // 将取出的pg重新插入pIndex
            if(pg != null) {
                pageIndex.add(pi.pgno, PageNormal.getFreeSpace(pg));
            } else {
                pageIndex.add(pi.pgno, freeSpace);
            }
        }

    }


    /*
     * 关闭DataManager
     */
    @Override
    public void close() {
        super.close();
        logger.close();

        PageOne.setVcClose(pageOne);
        pageOne.release();
        pc.close();
    }

    /*
     * 在创建文件时初始化PageOne并写入文件
     */
    public void initPageOne() {
        int pageNo=pc.newPage(PageOne.InitRaw());
        assert pageNo==1;
        try {
            pageOne=pc.getPage(1);
        } catch (Exception e) {
            Error.error(e);
        }
        pc.flushPage(pageOne);
    }

    /*
     * 加载检查PageOne
     */
    public boolean loadCheckPageOne() {
        try {
            pageOne=pc.getPage(1);
        } catch (Exception e) {
            Error.error(e);
        }
        return PageOne.checkVc(pageOne);

    }

    /*
     * 在打开已有文件时，加载PageIndex
     */
    public void loadPageIndex() {
        int pageNumber = pc.getPageNumber();
        for(int i = 2; i <= pageNumber; i ++) {
            Page pg = null;
            try {
                pg = pc.getPage(i);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            pageIndex.add(pg.getPageNumber(), PageNormal.getFreeSpace(pg));
            pg.release();
        }
    }

    /*
     * 生成updateLog
     */
    public void logDataItem(long xid, DataItem dataItem) {
        byte[] log = Recover.updateLog(xid, dataItem);
        logger.log(log);

    }

    public void releaseDataItem(DataItem dataItem) {
        super.release(dataItem.getUid());
    }
}
