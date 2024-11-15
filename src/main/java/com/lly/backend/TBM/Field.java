package com.lly.backend.TBM;


import com.google.common.primitives.Bytes;
import com.lly.backend.IM.BPlusTree;
import com.lly.backend.TBM.Result.ParseStringRes;
import com.lly.backend.TM.TransactionManagerImpl;
import com.lly.common.ErrorItem;
import com.lly.common.utils.Error;
import com.lly.common.utils.Parser;

import java.util.Arrays;

/**
 * field 表示字段信息
 * 二进制格式为：
 * [FieldName][TypeName][IndexUid]
 * 其中FieldName和TypeName为字节形式的字符串，存储方式为[StringLength][StringData]
 * 如果field无索引，IndexUid为0
 */
public class Field {
    long uid;
    private Table tb;
    String fieldName;
    String fieldType;
    private long indexUid;
    private BPlusTree bt;

    public Field(long uid, Table tb) {
        this.uid = uid;
        this.tb = tb;
    }
    public Field(Table tb, String fieldName, String fieldType, long index) {
        this.tb = tb;
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.indexUid = index;
    }

    /**
     * 从表中加载字段
     * @param tb 表
     * @param uid 字段uid
     * @return
     */
    public static Field loadField(Table tb, long uid) {
        byte[] raw = null;
        try {
            raw = ((TableManagerImpl)tb.tbm).vm.read(TransactionManagerImpl.SUPER_XID, uid);
        } catch (Exception e) {
            Error.error(e);
        }
        assert raw != null;
        return new Field(uid, tb).parseSelf(raw);

    }

    public static Field createField(Table tb, long xid, String fieldName, String fieldType, boolean indexed) throws Exception {
        typeCheck(fieldType);
        Field f = new Field(tb, fieldName, fieldType, 0);
        //创建B+树和加载B+树
        if(indexed) {
            long indexUid = BPlusTree.create(((TableManagerImpl)tb.tbm).dm);
            BPlusTree bt = BPlusTree.load(indexUid, ((TableManagerImpl)tb.tbm).dm);
            f.indexUid = indexUid;
            f.bt = bt;
        }
        //持久化字段
        f.persistSelf(xid);
        return f;
    }


    private void persistSelf(long xid) throws Exception {
        byte[] nameRaw = Parser.string2Byte(fieldName);
        byte[] typeRaw = Parser.string2Byte(fieldType);
        byte[] indexRaw = Parser.long2Byte(indexUid);
        //把字段通过vm包装为entry使用dm持久化写入到数据库
        this.uid = ((TableManagerImpl) tb.tbm).vm.insert(xid, Bytes.concat(nameRaw, typeRaw, indexRaw));
    }

    private static void typeCheck(String fieldType) throws Exception {
        if(!"int32".equals(fieldType) && !"int64".equals(fieldType) && !"string".equals(fieldType)) {
            throw ErrorItem.InvalidFieldException;
        }
    }

    /**
     * 字段类自己基于entry的uid解析出[FieldName][TypeName][IndexUid]，如果有索引则加载B+树
     * @param raw
     * @return
     */
    private Field parseSelf(byte[] raw) {
        int position = 0;
        ParseStringRes res=Parser.parseString(raw);
        fieldName = res.str;
        position += res.next;

        res = Parser.parseString(Arrays.copyOfRange(raw, position, raw.length));
        fieldType = res.str;
        position += res.next;

        this.indexUid = Parser.getLong(Arrays.copyOfRange(raw, position, position+8));
        if(indexUid != 0) {
            try {
                bt = BPlusTree.load(indexUid, ((TableManagerImpl)tb.tbm).dm);
            } catch(Exception e) {
                Error.error(e);
            }
        }
        return this;
    }


    public Object string2Value(String str) {
        return switch (fieldType) {
            case "int32" -> Integer.parseInt(str);
            case "int64" -> Long.parseLong(str);
            case "string" -> str;
            default -> null;
        };
    }

    public byte[] value2Raw(Object v) {
        return switch (fieldType) {
            case "int32" -> Parser.int2Byte((int) v);
            case "int64" -> Parser.long2Byte((long) v);
            case "string" -> Parser.string2Byte((String) v);
            default -> null;
        };
    }

    public boolean isIndexed() {
        return indexUid != 0;
    }

    public void insertIndex(Object FieldValue, long uid) throws Exception {
        long uKey = value2key(FieldValue);
        bt.insert(uKey, uid);
    }

    /**
     * 把字段值转换为索引的key
     * @param FieldValue
     * @return key
     */
    private long value2key(Object FieldValue) {
        long key = 0;
        switch(fieldType) {
            case "string":
                key = Parser.str2key((String) FieldValue);
                break;
            case "int32":
                int uint = (int) FieldValue;
                return (long)uint;
            case "int64":
                key = (long) FieldValue;
                break;
        }
        return key;
    }
}
