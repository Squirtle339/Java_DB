package com.lly.backend.TBM;


import com.google.common.primitives.Bytes;
import com.lly.backend.TBM.Result.FieldCalRes;
import com.lly.backend.TBM.Result.ParseStringRes;
import com.lly.backend.TM.TransactionManagerImpl;
import com.lly.backend.sqlParser.statement.Create;
import com.lly.backend.sqlParser.statement.Insert;
import com.lly.backend.sqlParser.statement.Select;
import com.lly.backend.sqlParser.statement.Where;
import com.lly.common.ErrorItem;
import com.lly.common.utils.Error;
import com.lly.common.utils.Parser;

import java.util.*;

/**
 * Table 维护了表结构
 * 二进制结构如下：
 * [TableName][NextTable]
 * [Field1Uid][Field2Uid]...[FieldNUid]
 */
public class Table {
    TableManager tbm;
    long uid;
    String name;
    byte status;
    long nextTableUid;
    List<Field> fields = new ArrayList<>();

    public Table(TableManager tbm, long uid) {
        this.tbm = tbm;
        this.uid = uid;
    }
    public Table(TableManager tbm, String tableName, long nextUid) {
        this.tbm = tbm;
        this.name = tableName;
        this.nextTableUid = nextUid;
    }

    public static Table loadTable(TableManager tbm, long uid) {
        byte[] raw = null;
        try {
            raw = ((TableManagerImpl)tbm).vm.read(TransactionManagerImpl.SUPER_XID, uid);
        } catch (Exception e) {
            Error.error(e);
        }
        Table tb = new Table(tbm, uid);
        return tb.parseSelf(raw);

    }

    public static Table createTable(TableManager tbm, long nextUid, long xid, Create create) throws Exception {
        Table tb = new Table(tbm, create.tableName, nextUid);
        for(int i = 0; i < create.fieldName.length; i ++) {
            String fieldName = create.fieldName[i];
            String fieldType = create.fieldType[i];
            boolean indexed = false;
            for(int j = 0; j < create.index.length; j ++) {
                if(fieldName.equals(create.index[j])) {
                    indexed = true;
                    break;
                }
            }
            tb.fields.add(Field.createField(tb, xid, fieldName, fieldType, indexed));
        }
        //持久化
        return tb.persistSelf(xid);
    }

    private Table persistSelf(long xid) throws Exception {
        byte[] nameRaw = Parser.string2Byte(name);
        byte[] nextRaw = Parser.long2Byte(nextTableUid);
        byte[] fieldRaw = new byte[0];
        for(Field field : fields) {
            fieldRaw = Bytes.concat(fieldRaw, Parser.long2Byte(field.uid));
        }
        uid = ((TableManagerImpl)tbm).vm.insert(xid, Bytes.concat(nameRaw, nextRaw, fieldRaw));
        return this;
    }

    private Table parseSelf(byte[] raw) {
        int position = 0;
        ParseStringRes res = Parser.parseString(raw);
        name = res.str;
        position += res.next;
        nextTableUid = Parser.getLong(Arrays.copyOfRange(raw, position, position+8));
        position += 8;
        while(position < raw.length) {
            long fieldUid = Parser.getLong(Arrays.copyOfRange(raw, position, position+8));
            position += 8;
            fields.add(Field.loadField(this, fieldUid));
        }
        return this;
    }

    public void insert(long xid, Insert insert) throws Exception {
        Map<String, Object> entry = string2Entry(insert.values);
        byte[] raw = entry2Raw(entry);
        long uid = ((TableManagerImpl)tbm).vm.insert(xid, raw);
        for(Field field : fields) {
            if(field.isIndexed()) {
                //插入索引，索引的key是字段值，value是存这一行数据的uid
                field.insertIndex(entry.get(field.fieldName), uid);
            }
        }
    }



    /**
     * 把values数组里的字符串数据根据字段类型转换为对应的数据类型
     * values数组只能是一行数据，所以和表里的字段一一对应
     */
    private Map<String, Object> string2Entry(String[] values) throws Exception {

        if(values.length != fields.size()) {
            throw ErrorItem.InvalidValuesException;
        }
        Map<String, Object> entry = new HashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            Field f = fields.get(i);
            Object v = f.string2Value(values[i]);
            entry.put(f.fieldName, v);
        }
        return entry;
    }

    /**
     * 把entry里的数据转换为二进制数据
     * @return [Field1Value][Field2Value]...[FieldNValue]
     */
    private byte[] entry2Raw(Map<String, Object> entry) {
        byte[] raw = new byte[0];
        for (Field field : fields) {
            raw = Bytes.concat(raw, field.value2Raw(entry.get(field.fieldName)));
        }
        return raw;
    }

    /**
     * select
     */
    public String read(long xid, Select select) throws Exception {
        List<Long> uids = parseWhere(select.where);
        StringBuilder sb = new StringBuilder();
        for (Long uid : uids) {
            byte[] raw = ((TableManagerImpl)tbm).vm.read(xid, uid);
            if(raw == null) continue;
            //提取字节形式行数据
            Map<String, Object> entry = parseEntry(raw);
            sb.append(printEntry(entry)).append("\n");
        }
        return sb.toString();
    }

    /**
     * 解析字节形式的行数据
     * @param raw 一行数据，字节数组格式
     * @return Map<fieldName,value>
     */
    private Map<String, Object> parseEntry(byte[] raw) {
        int pos = 0;
        Map<String, Object> entry = new HashMap<>();
        for (Field field : fields) {
            Field.ParseValueRes r =field.parserValue(Arrays.copyOfRange(raw, pos, raw.length));
            entry.put(field.fieldName, r.v);
            pos += r.shift;
        }
        return entry;
    }

    private String printEntry(Map<String, Object> entry) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            sb.append(field.printValue(entry.get(field.fieldName)));
            if(i == fields.size()-1) {
                sb.append("]");
            } else {
                sb.append(", ");
            }
        }
        return sb.toString();
    }

    private List<Long> parseWhere(Where where) throws Exception {
        long l0=0, r0=0, l1=0, r1=0;
        boolean single = false;
        Field fd = null;
        if(where==null) {
            //找到第一个有索引的字段
            for (Field field : fields) {
                if(field.isIndexed()) {
                    fd = field;
                    break;
                }
            }
            l0 = 0;
            r0 = Long.MAX_VALUE;

        }else{
            for(Field field: fields){
                if(field.fieldName.equals(where.singleExp1.fieldname)){
                    if (!field.isIndexed()){
                        throw  ErrorItem.FieldNotIndexedException;
                    }
                    fd = field;
                    break;
                }
            }
            if(fd == null) { // 如果没有找到匹配的字段，抛出异常
                throw ErrorItem.FieldNotFoundException;
            }
            CalWhereRes res = calWhere(fd, where);
            l0 = res.l0; r0 = res.r0;
            l1 = res.l1; r1 = res.r1;
            single = res.single;
        }
        List<Long> uids = fd.search(l0, r0);
        if(!single){
            List<Long> tmp = fd.search(l1, r1);
            uids.addAll(tmp);
        }
        return uids;
    }
    class CalWhereRes {
        long l0, r0, l1, r1;
        boolean single;
    }

    private CalWhereRes calWhere(Field fd, Where where) throws Exception {
        CalWhereRes res = new CalWhereRes();
        switch(where.logicOp) {
            case "":
                res.single = true;
                FieldCalRes r = fd.calExp(where.singleExp1);
                res.l0 = r.left; res.r0 = r.right;
                break;
            case "or":
                res.single = false;
                r=fd.calExp(where.singleExp1);
                res.l0 = r.left; res.r0 = r.right;
                r=fd.calExp(where.singleExp2);
                res.l1 = r.left; res.r1 = r.right;
                break;
            case "and":
                res.single = true;
                r = fd.calExp(where.singleExp1);
                res.l0 = r.left; res.r0 = r.right;
                r = fd.calExp(where.singleExp2);
                res.l1 = r.left; res.r1 = r.right;
                if(res.l1 > res.l0) res.l0 = res.l1;
                if(res.r1 < res.r0) res.r0 = res.r1;
                break;
            default:
                throw ErrorItem.InvalidLogOpException;
        }
        return res;
    }



}
