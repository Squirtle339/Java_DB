package com.lly.backend.DM;

import com.lly.backend.DM.dataItem.DataItem;
import com.lly.backend.DM.logger.Logger;
import com.lly.backend.DM.pageCache.PageCache;
import com.lly.backend.TM.TransactionManager;

public interface DataManager {

    DataItem read(long uid);
    long insert(long xid, byte[] data);

    void close();

    public static DataManager create(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.create(path, mem);
        Logger lg = Logger.create(path);

        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        dm.initPageOne();
        return dm;
    }
}
