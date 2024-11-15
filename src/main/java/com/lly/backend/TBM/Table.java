package com.lly.backend.TBM;


import com.google.common.primitives.Bytes;
import com.lly.backend.TBM.Result.ParseStringRes;
import com.lly.backend.TM.TransactionManagerImpl;
import com.lly.backend.sqlParser.statement.Create;
import com.lly.backend.sqlParser.statement.Insert;
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
}
