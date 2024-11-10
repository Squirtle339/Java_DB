package com.lly.backend.VM;

import com.lly.backend.TM.TransactionManager;

import java.util.concurrent.ThreadLocalRandom;

public class Visibility {

    /**
     * 判断是否版本跳跃
     * 版本跳跃的检查为：如果事务Ti要修改entry，但entry已经被Ti不可见的事务Tj修改过，则发生版本跳跃，需要回滚Ti
     * @param tm 事务管理器
     * @param t 事务
     * @param e 记录rntry
     * @return
     */
    public static boolean isVersionSkip(TransactionManager tm, Transaction t, Entry e) {
        long xmax = e.getXmax();
        //读提交级别的事务不会发生版本跳跃
        if(t.level == 0) {
            return false;
        } else {
            return tm.isCommitted(xmax) && (xmax > t.xid || t.isInSnapshot(xmax));
        }
    }

    /**
     * 判断记录是否对事务可见,会根据事务的隔离级别进行判断
     * @param tm 事务管理器
     * @param t 事务
     * @param entry 记录
     * @return
     */
    public static boolean isVisible(TransactionManager tm, Transaction t, Entry entry) {
        if(t.level == 0) {
            return readCommittedVisible(tm, t, entry);
        } else {
            return repeatableReadVisible(tm, t, entry);
        }
    }

    /**
     * 读提交级别的可见性判断
     * 【事务v只能读取已经提交事务产生的数据】
     * @param tm 事务管理器
     * @param t 事务
     * @param entry 记录
     * @return
     */
    private static boolean repeatableReadVisible(TransactionManager tm, Transaction t, Entry entry) {
        long xid = t.xid;
        long xmin = entry.getXmin();
        long xmax = entry.getXmax();
        //该记录是事务t创建的，并且未被修改
        if(xmin == xid && xmax == 0) return true;

        //该记录是已经提交的事务创建的
        if(tm.isCommitted(xmin)) {
            //该记录未被修改
            if (xmax == 0) return true;
            //该记录被其他事务修改过但这个事务还未提交（用于排除掉过时的记录）
            if (xmax != xid && !tm.isCommitted(xmax)) return true;
        }
        return false;
    }

    /**
     * 可重复读级别的可见性判断
     * 【事务只能读取它开始时, 就已经结束的那些事务产生的数据版本】
     * @param tm 事务管理器
     * @param t 事务
     * @param entry 记录
     * @return
     */
    private static boolean readCommittedVisible(TransactionManager tm, Transaction t, Entry entry) {
        long xid = t.xid;
        long xmin = entry.getXmin();
        long xmax = entry.getXmax();

        if(xmin == xid && xmax == 0) return true;
        //由已经提交事务创建，且该事务在事务t开始之前就已经提交
        if(tm.isCommitted(xmin) && xmin < xid && !t.isInSnapshot(xmin)) {
            //下面的进阶判断主要是为了排除掉过时的记录
            //未被修改
            if(xmax == 0) return true;
            //被其他事务修改过，但这个事务还未提交 or 在事务t之后创建 or 在事务t开始之前还没有提交（还是active）
            if(xmax != xid && (!tm.isCommitted(xmax) || xmax > xid || t.isInSnapshot(xmax))) {
                return true;
            }
        }
        return false;
    }
}
