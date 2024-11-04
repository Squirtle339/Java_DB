package com.lly.backend.DM;

import com.lly.backend.DM.dataItem.DataItem;
import com.lly.backend.DM.logger.Logger;
import com.lly.backend.DM.page.Page;
import com.lly.backend.DM.pageCache.PageCache;
import com.lly.backend.DM.pageIndex.PageIndex;
import com.lly.backend.TM.TransactionManager;
import com.lly.backend.common.AbstractCache;

public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager{
    TransactionManager tm;
    Logger lg;
    PageCache pc;

    PageIndex pIndex;
    Page pageOne;

    public DataManagerImpl(PageCache pc, Logger lg, TransactionManager tm) {
    }



    @Override
    protected DataItem getForCache(long key) throws Exception {
        return null;
    }

    @Override
    protected void releaseForCache(DataItem obj) {

    }


    @Override
    public DataItem read(long uid) {
        return null;
    }

    @Override
    public long insert(long xid, byte[] data) {
        return 0;
    }

    @Override
    public void close() {

    }
}
