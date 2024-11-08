package com.lly.backend.VM;

import com.lly.backend.DM.DataManager;
import com.lly.backend.TM.TransactionManager;
import com.lly.backend.TM.TransactionManagerImpl;
import com.lly.backend.common.AbstractCache;
import com.lly.common.ErrorItem;
import com.lly.common.utils.Error;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class VersionManagerImpl extends AbstractCache<Entry> implements VersionManager {



    TransactionManager tm;
    DataManager dm;
    Map<Long, Transaction> activeTransaction;
    Lock lock;//activeTransaction的锁
    LockTable lockTable;

    public VersionManagerImpl(TransactionManager tm, DataManager dm) {
        super(0);
        this.tm = tm;
        this.dm = dm;
        this.activeTransaction = new HashMap<>();
        activeTransaction.put(TransactionManagerImpl.SUPER_XID, Transaction.newTransaction(TransactionManagerImpl.SUPER_XID, 0, null));
        this.lock = new ReentrantLock();
        this.lockTable = new LockTable();
    }


    @Override
    public byte[] read(long xid, long uid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();
        if(t.err != null) {
            throw t.err;
        }

        Entry entry = super.get(uid);
        try {
            //判断记录是否对当前事务可见
            if(Visibility.isVisible(tm, t, entry)) {
                return entry.data();
            } else {
                return null;
            }
        } finally {
            entry.release();
        }
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        return 0;
    }

    @Override
    public boolean delete(long xid, long uid) throws Exception {
        return false;
    }

    @Override
    public long begin(int level) {
        lock.lock();
        try {
            //事务管理器生成事务id
            long xid = tm.begin();
            activeTransaction.put(xid, Transaction.newTransaction(xid, level, activeTransaction));
            return xid;
        } finally {
            lock.unlock();
        }

    }


    /*
    * 提交事务,将事务从活跃事务哈希表中移除
    */
    @Override
    public void commit(long xid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        try {
            if(t.err != null) {
                throw t.err;
            }
        } catch(NullPointerException n) {
            System.out.println(xid);
            System.out.println(activeTransaction.keySet());
            Error.error(n);
        }

        lock.lock();
        activeTransaction.remove(xid);
        lock.unlock();

        lockTable.remove(xid);
        tm.commit(xid);
    }

    /*
    * 手动终止事务
     */
    @Override
    public void abort(long xid) {
        internAbort(xid, false);
    }

    //自动终止事务
    private void internAbort(long xid, boolean autoAborted) {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        if(!autoAborted) {
            activeTransaction.remove(xid);
        }
        lock.unlock();

        if(t.autoAborted) return;

        lockTable.remove(xid);
        tm.abort(xid);
    }

    /*
     * 从缓存中获取entry
     */
    @Override
    protected Entry getForCache(long uid) throws Exception {
        Entry entry = Entry.loadEntry(this, uid);
        if (entry==null){
            throw ErrorItem.NullEntryException;
        }
        return entry;
    }

    @Override
    protected void releaseForCache(Entry entry) {
        entry.remove();
    }
}
