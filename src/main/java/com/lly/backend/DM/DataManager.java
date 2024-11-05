package com.lly.backend.DM;

import com.lly.backend.DM.dataItem.DataItem;
import com.lly.backend.DM.logger.Logger;
import com.lly.backend.DM.page.PageOne;
import com.lly.backend.DM.pageCache.PageCache;
import com.lly.backend.DM.pageCache.PageCacheImpl;
import com.lly.backend.TM.TransactionManager;

public interface DataManager {

    DataItem read(long uid) throws Exception;
    long insert(long xid, byte[] data) throws Exception;

    void close();

    public static DataManager create(String path, long memory, TransactionManager tm) {
        PageCache pc = PageCache.create(path, memory);
        Logger lg = Logger.create(path);
        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);

        dm.initPageOne();
        return dm;
    }

    public static DataManager open(String path, long memory, TransactionManager tm) {
        PageCache pageCache = PageCache.open(path, memory);
        Logger lg = Logger.open(path);
        DataManagerImpl dm = new DataManagerImpl(pageCache, lg, tm);

        if(!dm.loadCheckPageOne()) {
            Recover.recover(tm, lg, pageCache);
        }
        //初始化pageIndex
        dm.loadPageIndex();
        PageOne.setVcOpen(dm.pageOne);
        dm.pc.flushPage(dm.pageOne);
        return dm;
    }


}
