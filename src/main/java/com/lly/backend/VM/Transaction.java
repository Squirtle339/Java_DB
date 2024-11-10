package com.lly.backend.VM;


import com.lly.backend.TM.TransactionManagerImpl;

import java.util.HashMap;
import java.util.Map;

/**
 *  VM对一个事务的抽象
 */
public class Transaction {

    public long xid;
    public int level;
    //记录事务开始时还是active的事务
    public Map<Long, Boolean> snapshot;
    public Exception err;
    public boolean autoAborted;

    public static Transaction newTransaction(long xid, int level, Map<Long, Transaction> active){
        Transaction t = new Transaction();
        t.xid = xid;
        t.level = level;
        if(level != 0) {
            t.snapshot = new HashMap<>();
            for(Long x : active.keySet()) {
                t.snapshot.put(x, true);
            }
        }
        return t;
    }

    public boolean isInSnapshot(long xid) {
        if(xid == TransactionManagerImpl.SUPER_XID) {
            return false;
        }
        return snapshot.containsKey(xid);
    }
}
