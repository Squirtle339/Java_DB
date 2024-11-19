package com.lly.backend.VM;


import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 *维护一个资源配图，用于死锁检测
 */
public class LockTable {
    private Map<Long, List<Long>> x2u;  // 某个XID已经获得的资源的UID列表
    private Map<Long, Long> u2x;        // UID被某个XID持有
    private Map<Long, List<Long>> xListWaitU; // 正在等待UID的XID列表
    private Map<Long, Lock> waitLock;   // XID-正在等待资源的锁
    private Map<Long, Long> xWaitU;      // XID正在等待的UID
    private Lock lock; // 资源分配图的锁

    public LockTable() {
        x2u = new HashMap<>();
        u2x = new HashMap<>();
        xListWaitU = new HashMap<>();
        waitLock = new HashMap<>();
        xWaitU = new HashMap<>();
        lock = new ReentrantLock();
    }
    /**
     * 事务尝试获取资源
     * 不需要等待则返回null，否则在资源分配图中添加边，进行死锁检测，如果会造成死锁则抛出异常，没有死锁则返回锁对象
     * @param xid
     * @param uid
     * @return
     */
    public Lock add(long xid, long uid) throws Exception {
        lock.lock();
        try{
            // 如果事务已经持有资源，则直接返回null
            if(isInList(x2u, xid, uid)) {
                return null;
            }
            // 如果资源没有被持有，则直接分配资源
            if(!u2x.containsKey(uid)) {
                u2x.put(uid, xid);
                x2u.putIfAbsent(xid, new ArrayList<>());
                x2u.get(xid).add(uid);
                return null;
            }
            // 如果资源被其他事务持有，则在资源分配图中添加边
            xWaitU.put(xid, uid);
            xListWaitU.putIfAbsent(uid, new ArrayList<>());
            xListWaitU.get(uid).add(xid);
            if (hasDeadLock()) {
                xWaitU.remove(xid);
                xListWaitU.get(uid).remove(xid);
                throw new Exception("Deadlock");
            }
            // 返回锁对象
            Lock l = new ReentrantLock();
            l.lock();
            waitLock.put(xid, l);
            return l;
        }
        finally {
            lock.unlock();
        }
    }


    private Map<Long, Integer> xidStamp;
    private int stamp;

    /**
     * 判断是否存在死锁,即判断是否存在环
     * @return
     */
    private boolean hasDeadLock() {
        xidStamp = new HashMap<>();
        stamp = 1;
        for(long xid : x2u.keySet()) {
            Integer s = xidStamp.get(xid);
            if(s != null && s > 0) {
                continue;
            }
            stamp ++;
            if(dfs(xid)) {
                return true;
            }
        }
        return false;
    }

    private boolean dfs(long xid) {
        Integer stp = xidStamp.get(xid);
        if(stp != null && stp == stamp) {
            return true;
        }
        if(stp != null && stp < stamp) {
            return false;
        }
        xidStamp.put(xid, stamp);

        Long uid = xWaitU.get(xid);
        if(uid == null) return false;
        Long x = u2x.get(uid);
        assert x != null;
        return dfs(x);
    }

    private boolean isInList(Map<Long, List<Long>> x2u, long xid, long uid) {
        for(long id: x2u.get(xid)) {
            if(id == uid) {
                return true;
            }
        }
        return false;

    }


    /**
     * 当一个事务commit或abort时，释放所持有的资源的锁
     * @param xid
     */
    public void remove(long xid) {
        lock.lock();
        try{
            List<Long> list = x2u.get(xid);
            for(long uid: list) {
                u2x.remove(uid);
                //从等待队列中选择一个新的事务来占用uid
                selectNewXID(uid);
            }
            x2u.remove(xid);
            xWaitU.remove(xid);
            waitLock.remove(xid);
        }
        finally {
            lock.unlock();
        }
    }


    /**
     * 从资源的等待队列中选择一个xid来占用uid
     * @param uid
     */
    private void selectNewXID(long uid) {
        List<Long> waitlist = xListWaitU.get(uid);
        if(waitlist == null||waitlist.isEmpty()) return;
        while(!waitlist.isEmpty()) {
            long xid = waitlist.remove(0);
            if(!waitLock.containsKey(xid)) {
                continue;
            } else {
                u2x.put(uid, xid);
                Lock lo = waitLock.remove(xid);
                xWaitU.remove(xid);
                lo.unlock();
                break;
            }
        }

        if(waitlist.isEmpty()) xListWaitU.remove(uid);
    }
}
