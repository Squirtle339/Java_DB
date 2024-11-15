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


    /**
     * 事务读取一个记录的数据
     * @param xid 事务id
     * @param uid 记录id
     * @return  记录的数据
     * @throws Exception
     */
    @Override
    public byte[] read(long xid, long uid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();
        if(t.err != null) {
            throw t.err;
        }

        //获取entry
        Entry entry = null;
        try {
            entry = super.get(uid);
        } catch(Exception e) {
            if(e == ErrorItem.NullEntryException) {
                return null;
            } else {
                throw e;
            }
        }

        try {
            //判断记录是否对当前事务可见
            if(Visibility.isVisible(tm, t, entry)) {
                return entry.data();
            } else {
                return null;
            }
        } finally {
            //entry缓存的引用计数减一
            entry.release();
        }
    }


    /**
     * 事务把数据包装成entry使用DM插入数据库
     * @param xid
     * @param data
     * @return
     * @throws Exception
     */
    @Override
    public long insert(long xid, byte[] data) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }
        byte[] raw = Entry.wrapEntryRaw(xid, data);
        return dm.insert(xid, raw);
    }


    /**
     * 事务删除记录
     * @param xid
     * @param uid
     * @return
     * @throws Exception
     */
    @Override
    public boolean delete(long xid, long uid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }

        Entry entry = null;
        try {
            entry = super.get(uid);
        } catch(Exception e) {
            if(e == ErrorItem.NullEntryException) {
                return false;
            } else {
                throw e;
            }
        }

        try {
            //不可见，事务删除记录失败
            if(!Visibility.isVisible(tm, t, entry)) {
                return false;
            }

            Lock l = null;
            //事务尝试获取记录的锁，在资源分配图中加入一条边，进行死锁检测
            try {
                l = lockTable.add(xid, uid);
            } catch(Exception e) {
                //发生死锁，事务删除失败，要求事务回滚
                t.err = ErrorItem.ConcurrentUpdateException;
                internAbort(xid, true);
                t.autoAborted = true;
                throw t.err;
            }

            if(l != null) {
                l.lock();
                l.unlock();
            }

            if(entry.getXmax() == xid) {
                return false;
            }
            //发生版本跳跃，事务删除失败，要求事务回滚
            if(Visibility.isVersionSkip(tm, t, entry)) {
                t.err = ErrorItem.ConcurrentUpdateException;
                internAbort(xid, true);
                t.autoAborted = true;
                throw t.err;
            }
            //设置记录的xmax，表示记录被删除
            entry.setXmax(xid);
            return true;
        }
        finally {
            //entry缓存的引用计数减一
            entry.release();

        }
    }

    /**
     * 开启事务管理器
     * @param level 事务隔离级别,1为可重复读，0为读已提交
     * @return
     */
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


    /**
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

    /**
    * 手动终止事务
     */
    @Override
    public void abort(long xid) {
        internAbort(xid, false);
    }

    /**
     * 事务终止
     * @param xid
     * @param autoAborted
     */
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

    /**
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

    /**
     * 释放entry所在的dataItem的缓存
     */
    @Override
    protected void releaseForCache(Entry entry) {
        entry.remove();
    }

    /**
     * 释放entry的一个缓存引用计数
     */
    public void releaseEntry(Entry entry) {
        super.release(entry.getUid());
    }
}
