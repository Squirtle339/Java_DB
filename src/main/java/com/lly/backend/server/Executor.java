package com.lly.backend.server;

import com.lly.backend.TBM.Result.BeginRes;
import com.lly.backend.TBM.TableManager;
import com.lly.backend.sqlParser.sqlParser;
import com.lly.backend.sqlParser.statement.*;
import com.lly.common.ErrorItem;

/**
 * server的核心，负责执行sql语句
 */
public class Executor {
    private long xid;
    TableManager tbm;

    public Executor(TableManager tbm) {
        this.tbm = tbm;
        this.xid = 0;
    }

    public void close() {
        if(xid != 0) {
            System.out.println("Abnormal Abort: " + xid);
            tbm.abort(xid);
        }
    }

    public byte[] execute(byte[] sql) throws Exception {
        System.out.println("Execute: " + new String(sql));
        Object stat = sqlParser.Parse(sql);
        //检查stat是否是Begin类的实例
        if(Begin.class.isInstance(stat)) {
            if(xid != 0) {
                throw ErrorItem.NestedTransactionException;
            }
            //由超级事务创建新事务，将新事务的xid赋值给实例的xid
            BeginRes r = tbm.begin((Begin)stat);
            xid = r.xid;
            return r.result;
        } else if(Commit.class.isInstance(stat)) {
            if(xid == 0) {
                throw ErrorItem.NoTransactionException;
            }
            byte[] res = tbm.commit(xid);
            xid = 0;
            return res;
        } else if(Abort.class.isInstance(stat)) {
            if(xid == 0) {
                throw ErrorItem.NoTransactionException;
            }
            byte[] res = tbm.abort(xid);
            xid = 0;
            return res;
        } else {//普通的增删改查操作
            return executeNormal(stat);
        }
    }

    private byte[] executeNormal(Object stat) throws Exception {
        //对于没有事务的sql语句，需要创建一个临时事务，执行完毕后立刻提交或者回滚
        boolean tmpTransaction = false;
        Exception e = null;
        if(xid == 0) {
            tmpTransaction = true;
            BeginRes r = tbm.begin(new Begin());
            xid = r.xid;
        }
        try {
            byte[] res = null;
            if(Show.class.isInstance(stat)) {
                res = tbm.show(xid);
            } else if(Create.class.isInstance(stat)) {
                res = tbm.create(xid, (Create)stat);
            } else if(Select.class.isInstance(stat)) {
                res = tbm.read(xid, (Select)stat);
            } else if(Insert.class.isInstance(stat)) {
                res = tbm.insert(xid, (Insert)stat);
            } else if(Delete.class.isInstance(stat)) {
                res = tbm.delete(xid, (Delete)stat);
            } else if(Update.class.isInstance(stat)) {
                res = tbm.update(xid, (Update)stat);
            }
            return res;
        }catch (Exception e1){
            e = e1;
            throw e;
        }finally {
            if(tmpTransaction) {
                if(e != null) {
                    tbm.abort(xid);
                } else {
                    tbm.commit(xid);
                }
                xid = 0;
            }
        }
    }

}
