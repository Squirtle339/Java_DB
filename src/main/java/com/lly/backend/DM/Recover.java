package com.lly.backend.DM;

import com.google.common.primitives.Bytes;
import com.lly.backend.DM.dataItem.DataItem;
import com.lly.backend.DM.logger.Logger;
import com.lly.backend.DM.page.Page;
import com.lly.backend.DM.page.PageNormal;
import com.lly.backend.DM.pageCache.PageCache;
import com.lly.backend.TM.TransactionManager;
import com.lly.backend.common.MySubArray;
import com.lly.common.utils.Error;
import com.lly.common.utils.Parser;

import java.util.*;

public class Recover {
    //日志类型
    private static final byte LOG_TYPE_INSERT = 0;
    private static final byte LOG_TYPE_UPDATE = 1;

    //恢复类型
    private static final int REDO = 0;
    private static final int UNDO = 1;

    // [LogType] [XID] [UID] [OldRaw] [NewRaw] -> [0][1:8][9:16][17:][]
    private static final int OF_TYPE = 0;//0
    private static final int OF_XID = OF_TYPE+1;//1
    private static final int OF_UPDATE_UID = OF_XID+8;//9
    private static final int OF_UPDATE_RAW = OF_UPDATE_UID+8;//17

    // [LogType] [XID] [Pgno] [Offset] [Raw] -> [0][1:8][9:12][13:15][16:-1]
    private static final int OF_INSERT_PGNO = OF_XID+8;//9
    private static final int OF_INSERT_OFFSET = OF_INSERT_PGNO+4;//13
    private static final int OF_INSERT_RAW = OF_INSERT_OFFSET+2;//15



    static class InsertLogInfo {
        long xid;
        int pgno;
        short offset;
        byte[] raw;
    }
    static class UpdateLogInfo {
        long xid;
        int pgno;
        short offset;
        byte[] oldRaw;
        byte[] newRaw;
    }
    public static void recover(TransactionManager tm, Logger lg, PageCache pc) {
        // 开始恢复过程
        System.out.println("Recovering...");

        // 将日志回滚到开头
        lg.rewind();
        int maxPgno = 0;

        // 遍历所有日志条目以找到最大页号
        while(true) {
            byte[] log = lg.next();
            if(log == null) break;
            int pgno;
            if(isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                pgno = li.pgno;
            } else {
                UpdateLogInfo li = parseUpdateLog(log);
                pgno = li.pgno;
            }
            if(pgno > maxPgno) {
                maxPgno = pgno;
            }
        }

        // 确保至少存在一页，因为第一页为特殊页不会出现在日志中
        if(maxPgno == 0) {
            maxPgno = 1;
        }

        // 将页面缓存截断到最大页号
        pc.truncateByPgno(maxPgno);
        System.out.println("Truncate to " + maxPgno + " pages.");

        // 重做事务
        redoTranscations(tm, lg, pc);
        System.out.println("Redo Transactions Over.");

        // 撤销事务
        undoTranscations(tm, lg, pc);
        System.out.println("Undo Transactions Over.");

        // 结束恢复过程
        System.out.println("Recovery Over.");
    }



    /*
     *重做已经提交的事务
     */
    private static void redoTranscations(TransactionManager tm, Logger lg, PageCache pageCache){
        lg.rewind();
        while(true){
            byte[] log = lg.next();
            if(log == null) break;
            if(isInsertLog(log)){
                InsertLogInfo insertInfo = parseInsertLog(log);
                long xid = insertInfo.xid;
                //如果事务为commit或者abort状态
                if(!tm.isAlive(xid)){
                    doInsertLog(pageCache,insertInfo,REDO);
                }
            }else {
                UpdateLogInfo updateInfo = parseUpdateLog(log);
                long xid = updateInfo.xid;
                //如果事务为commit或者abort状态
                if(!tm.isAlive(xid)){
                    doUpdateLog(pageCache,updateInfo,REDO);
                }
            }
        }
    }

    private static void undoTranscations(TransactionManager tm, Logger lg, PageCache pc){
        Map<Long, List<byte[]>> logCache = new HashMap<>();
        lg.rewind();
        while (true){
            byte[] log = lg.next();
            if(log == null) break;
            if(isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                long xid = li.xid;
                if(tm.isAlive(xid)){
                    logCache.getOrDefault(xid, new ArrayList<>()).add(log);
                }
            }
            else {
                UpdateLogInfo xi = parseUpdateLog(log);
                long xid = xi.xid;
                if(tm.isAlive(xid)){
                    logCache.getOrDefault(xid, new ArrayList<>()).add(log);
                }
            }
        }

        // 进行active的事务的log进行倒序undo
        for(Map.Entry<Long, List<byte[]>> entry : logCache.entrySet()) {
            List<byte[]> logs = entry.getValue();
            for (int i = logs.size()-1; i >= 0; i --) {
                byte[] log = logs.get(i);
                if(isInsertLog(log)) {
                    doInsertLog(pc, parseInsertLog(log), UNDO);
                } else {
                    doUpdateLog(pc, parseUpdateLog(log), UNDO);
                }
            }
            tm.abort(entry.getKey());
        }
    }

    private static void doUpdateLog(PageCache pageCache, UpdateLogInfo updateInfo, int flag) {
        int pgno=updateInfo.pgno;
        short offset=updateInfo.offset;
        byte[] raw;
        if(flag==UNDO) {
            raw = updateInfo.oldRaw;
        }else{
            raw = updateInfo.newRaw;
        }
        Page pg = null;
        try {
            pg = pageCache.getPage(pgno);
        } catch (Exception e) {
            Error.error(e);
        }

        try {
            PageNormal.recoverUpdate(pg, raw, offset);
        } finally {
            pg.release();
        }
    }

    /*
     *redo或者undo一个commit或者abort的事务中的一条Insert日志
     */
    private static void doInsertLog(PageCache pageCache, InsertLogInfo logInfo, int flag) {
        Page pg = null;
        try {
            pg = pageCache.getPage(logInfo.pgno);
        } catch (Exception e) {
            Error.error(e);
        }
        try {
            if(flag==UNDO){
                //逻辑删除
                DataItem.setDataItemRawInvalid(logInfo.raw);
            }
            PageNormal.recoverInsert(pg, logInfo.raw, logInfo.offset);
        }
        finally {
            pg.release();
        }
    }

    private static UpdateLogInfo parseUpdateLog(byte[] log) {
        //[LogType] [XID] [UID] [OldRaw] [NewRaw]
        UpdateLogInfo li = new UpdateLogInfo();
        li.xid = Parser.getLong(Arrays.copyOfRange(log, OF_XID, OF_UPDATE_UID));
        long uid = Parser.getLong(Arrays.copyOfRange(log, OF_UPDATE_UID, OF_UPDATE_RAW));
        // 从UID中提取低16位作为offset
        li.offset = (short)(uid & ((1L << 16) - 1));
        // 从UID中提取低32-64位作为pgno
        uid >>>= 32;
        li.pgno = (int)(uid & ((1L << 32) - 1));
        // 计算oldRaw和newRaw的长度，因为他们类型相同，长度相同
        int length = (log.length - OF_UPDATE_RAW) / 2;
        li.oldRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW, OF_UPDATE_RAW+length);
        li.newRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW+length, OF_UPDATE_RAW+length*2);
        return li;
    }

    private static InsertLogInfo parseInsertLog(byte[] log) {

        InsertLogInfo insertLogInfo = new InsertLogInfo();
        insertLogInfo.xid = Parser.getLong(Arrays.copyOfRange(log, OF_XID, OF_INSERT_PGNO));
        insertLogInfo.pgno = Parser.getInt(Arrays.copyOfRange(log, OF_INSERT_PGNO, OF_INSERT_OFFSET));
        insertLogInfo.offset = Parser.getShort(Arrays.copyOfRange(log, OF_INSERT_OFFSET, OF_INSERT_RAW));
        insertLogInfo.raw = Arrays.copyOfRange(log, OF_INSERT_RAW, log.length);
        return insertLogInfo;
    }

    private static boolean isInsertLog(byte[] log) {
        return log[0] == LOG_TYPE_INSERT;
    }

    /*
     *生成一条insert日志
     */
    public static byte[] insertLog(long xid, Page pg, byte[] raw) {
        byte[] logTypeRaw = {LOG_TYPE_INSERT};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] pgnoRaw = Parser.int2Byte(pg.getPageNumber());
        byte[] offsetRaw = Parser.short2Byte(PageNormal.getFSO(pg));
        return Bytes.concat(logTypeRaw, xidRaw, pgnoRaw, offsetRaw, raw);
    }

    public static byte[] updateLog(long xid, DataItem di) {
        byte[] logType = {LOG_TYPE_UPDATE};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] uidRaw = Parser.long2Byte(di.getUid());
        byte[] oldRaw = di.getOldRaw();
        MySubArray raw = di.getRaw();
        byte[] newRaw = Arrays.copyOfRange(raw.raw, raw.start, raw.end);
        return Bytes.concat(logType, xidRaw, uidRaw, oldRaw, newRaw);
    }


}
