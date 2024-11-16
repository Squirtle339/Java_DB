package com.lly.backend.sqlParser;

import com.lly.backend.sqlParser.statement.*;
import com.lly.common.ErrorItem;

import java.util.ArrayList;
import java.util.List;

/**
 * 简易的sql解析器，解析sql语句为对应的对象
 * 根据定义的语法规则，匹配语句中的关键字，解析出对应的对象
 */
public class sqlParser {

    public static Object Parse(byte[] statement) throws Exception {
        Tokenizer tokenizer = new Tokenizer(statement);
        String token = tokenizer.peek();
        tokenizer.pop();

        Object statementObj = null;
        Exception statementErr = null;

        try{

            statementObj = switch (token) {
                case "begin" -> parseBegin(tokenizer);
                case "commit" -> parseCommit(tokenizer);
                case "abort" -> parseAbort(tokenizer);
                case "create" -> parseCreate(tokenizer);
                case "drop" -> parseDrop(tokenizer);
                case "select" -> parseSelect(tokenizer);
                case "insert" -> parseInsert(tokenizer);
                case "delete" -> parseDelete(tokenizer);
                case "update" -> parseUpdate(tokenizer);
                case "show" -> parseShow(tokenizer);
                default -> throw ErrorItem.InvalidCommandException;
            };
        }catch (Exception e){
            statementErr = e;
        }

        //若还有未解析的字符，抛出异常，语句不合法
        try {
            String next = tokenizer.peek();
            if(!"".equals(next)) {
                byte[] errStat = tokenizer.errStat();
                statementErr = new RuntimeException("Invalid statement: " + new String(errStat));
            }
        } catch(Exception e) {
            e.printStackTrace();
            byte[] errStat = tokenizer.errStat();
            statementErr = new RuntimeException("Invalid statement: " + new String(errStat));
        }

        return statementObj;
    }
    private static Show parseShow(Tokenizer tokenizer) throws Exception {
        String tmp = tokenizer.peek();
        if("".equals(tmp)) {
            return new Show();
        }
        throw ErrorItem.InvalidCommandException;
    }

    private static Update parseUpdate(Tokenizer tokenizer) throws Exception {
        Update update = new Update();
        update.tableName = tokenizer.peek();
        tokenizer.pop();

        if(!"set".equals(tokenizer.peek())) {
            throw ErrorItem.InvalidCommandException;
        }
        tokenizer.pop();

        update.fieldName = tokenizer.peek();
        tokenizer.pop();

        if(!"=".equals(tokenizer.peek())) {
            throw ErrorItem.InvalidCommandException;
        }
        tokenizer.pop();

        update.value = tokenizer.peek();
        tokenizer.pop();

        String tmp = tokenizer.peek();
        if("".equals(tmp)) {
            update.where = null;
            return update;
        }

        update.where = parseWhere(tokenizer);
        return update;
    }

    private static Delete parseDelete(Tokenizer tokenizer) throws Exception {
        Delete delete = new Delete();

        if(!"from".equals(tokenizer.peek())) {
            throw ErrorItem.InvalidCommandException;
        }
        tokenizer.pop();

        String tableName = tokenizer.peek();
        if(!isName(tableName)) {
            throw ErrorItem.InvalidCommandException;
        }
        delete.tableName = tableName;
        tokenizer.pop();

        delete.where = parseWhere(tokenizer);
        return delete;
    }

    private static Insert parseInsert(Tokenizer tokenizer) throws Exception {
        Insert insert = new Insert();

        if(!"into".equals(tokenizer.peek())) {
            throw ErrorItem.InvalidCommandException;
        }
        tokenizer.pop();

        String tableName = tokenizer.peek();
        if(!isName(tableName)) {
            throw ErrorItem.InvalidCommandException;
        }
        insert.tableName = tableName;
        tokenizer.pop();

        if(!"values".equals(tokenizer.peek())) {
            throw ErrorItem.InvalidCommandException;
        }

        List<String> values = new ArrayList<>();
        while(true) {
            tokenizer.pop();
            String value = tokenizer.peek();
            if("".equals(value)) {
                break;
            } else {
                values.add(value);
            }
        }
        insert.values = values.toArray(new String[values.size()]);

        return insert;
    }
    private static Select parseSelect(Tokenizer tokenizer) throws Exception {
        Select read = new Select();

        List<String> fields = new ArrayList<>();
        String asterisk = tokenizer.peek();
        if("*".equals(asterisk)) {
            fields.add(asterisk);
            tokenizer.pop();
        } else {
            while(true) {
                String field = tokenizer.peek();
                if(!isName(field)) {
                    throw ErrorItem.InvalidCommandException;
                }
                fields.add(field);
                tokenizer.pop();
                if(",".equals(tokenizer.peek())) {
                    tokenizer.pop();
                } else {
                    break;
                }
            }
        }
        read.fields = fields.toArray(new String[fields.size()]);

        if(!"from".equals(tokenizer.peek())) {
            throw ErrorItem.InvalidCommandException;
        }
        tokenizer.pop();

        String tableName = tokenizer.peek();
        if(!isName(tableName)) {
            throw ErrorItem.InvalidCommandException;
        }
        read.tableName = tableName;
        tokenizer.pop();

        String tmp = tokenizer.peek();
        if("".equals(tmp)) {
            read.where = null;
            return read;
        }

        read.where = parseWhere(tokenizer);
        return read;
    }


    /**
     * 解析where条件，简易的where条件只包含最多两个表达式
     * where <field name> (><=) <value> [(andor) <field name> (><=) <value>]
     */
    private static Where parseWhere(Tokenizer tokenizer) throws Exception {
        Where where = new Where();

        if(!"where".equals(tokenizer.peek())) {
            throw ErrorItem.InvalidCommandException;
        }
        tokenizer.pop();

        SingleExpression exp1 = parseSingleExp(tokenizer);
        where.singleExp1 = exp1;

        String logicOp = tokenizer.peek();
        if("".equals(logicOp)) {
            where.logicOp = logicOp;
            return where;
        }

        if(!isLogicOp(logicOp)) {
            throw ErrorItem.InvalidCommandException;
        }
        where.logicOp = logicOp;
        tokenizer.pop();

        SingleExpression exp2 = parseSingleExp(tokenizer);
        where.singleExp2 = exp2;

        if(!"".equals(tokenizer.peek())) {
            throw ErrorItem.InvalidCommandException;
        }
        return where;
    }

    /**
     * 解析where条件表达式的单个表达式
     * <field name> >\<\= <value>
     */
    private static SingleExpression parseSingleExp(Tokenizer tokenizer) throws Exception {
        SingleExpression exp = new SingleExpression();

        String field = tokenizer.peek();
        if(!isName(field)) {
            throw ErrorItem.InvalidCommandException;
        }
        exp.fieldname = field;
        tokenizer.pop();

        String op = tokenizer.peek();
        //判断是否为比较运算符
        if(!isCmpOp(op)) {
            throw ErrorItem.InvalidCommandException;
        }
        exp.compareOp = op;
        tokenizer.pop();

        exp.value = tokenizer.peek();
        tokenizer.pop();
        return exp;
    }


    private static Drop parseDrop(Tokenizer tokenizer) throws Exception {
        if(!"table".equals(tokenizer.peek())) {
            throw ErrorItem.InvalidCommandException;
        }
        tokenizer.pop();

        String tableName = tokenizer.peek();
        if(!isName(tableName)) {
            throw ErrorItem.InvalidCommandException;
        }
        tokenizer.pop();

        if(!"".equals(tokenizer.peek())) {
            throw ErrorItem.InvalidCommandException;
        }

        Drop drop = new Drop();
        drop.tableName = tableName;
        return drop;
    }

    private static Object parseCreate(Tokenizer tokenizer) throws Exception {
        if(!"table".equals(tokenizer.peek())) {
            throw ErrorItem.InvalidCommandException;
        }
        tokenizer.pop();
        Create create = new Create();
        String name = tokenizer.peek();
        //表名要求长度大于1且第一个字符为字母
        if(!isName(name)) {
            throw ErrorItem.InvalidCommandException;
        }
        create.tableName = name;
        List<String> fNames = new ArrayList<>();
        List<String> fTypes = new ArrayList<>();
        while(true) {
            tokenizer.pop();
            String field = tokenizer.peek();
            if("(".equals(field)) {
                break;
            }

            if(!isName(field)) {
                throw ErrorItem.InvalidCommandException;
            }

            tokenizer.pop();
            String fieldType = tokenizer.peek();
            if(!isType(fieldType)) {
                throw ErrorItem.InvalidCommandException;
            }
            fNames.add(field);
            fTypes.add(fieldType);
            tokenizer.pop();

            String next = tokenizer.peek();
            if(",".equals(next)) {
                continue;
            } else if("".equals(next)) {
                throw ErrorItem.TableNoIndexException;
            } else if("(".equals(next)) {
                break;
            } else {
                throw ErrorItem.InvalidCommandException;
            }
        }
        create.fieldName = fNames.toArray(new String[fNames.size()]);
        create.fieldType = fTypes.toArray(new String[fTypes.size()]);

        tokenizer.pop();
        if(!"index".equals(tokenizer.peek())) {
            throw ErrorItem.InvalidCommandException;
        }

        List<String> indexes = new ArrayList<>();
        while(true) {
            tokenizer.pop();
            String field = tokenizer.peek();
            if(")".equals(field)) {
                break;
            }
            if(!isName(field)) {
                throw ErrorItem.InvalidCommandException;
            } else {
                indexes.add(field);
            }
        }
        create.index = indexes.toArray(new String[indexes.size()]);
        tokenizer.pop();

        if(!"".equals(tokenizer.peek())) {
            throw ErrorItem.InvalidCommandException;
        }
        return create;
    }

    private static Abort parseAbort(Tokenizer tokenizer) throws Exception {
        if(!"".equals(tokenizer.peek())) {
            throw ErrorItem.InvalidCommandException;
        }
        return new Abort();
    }
    private static Object parseCommit(Tokenizer tokenizer) throws Exception {
        if(!"".equals(tokenizer.peek())) {
            throw ErrorItem.InvalidCommandException;
        }
        return new Commit();
    }

    private static Begin parseBegin(Tokenizer tokenizer) throws Exception {
        String isolation = tokenizer.peek();
        Begin begin = new Begin();
        if("".equals(isolation)) {
            return begin;
        }
        if(!"isolation".equals(isolation)) {
            throw ErrorItem.InvalidCommandException;
        }

        tokenizer.pop();
        String level = tokenizer.peek();
        if(!"level".equals(level)) {
            throw ErrorItem.InvalidCommandException;
        }
        tokenizer.pop();

        String tmp1 = tokenizer.peek();
        if("read".equals(tmp1)) {
            tokenizer.pop();
            String tmp2 = tokenizer.peek();
            if("committed".equals(tmp2)) {
                tokenizer.pop();
                if(!"".equals(tokenizer.peek())) {
                    throw ErrorItem.InvalidCommandException;
                }
                return begin;
            } else {
                throw ErrorItem.InvalidCommandException;
            }
        } else if("repeatable".equals(tmp1)) {
            tokenizer.pop();
            String tmp2 = tokenizer.peek();
            if("read".equals(tmp2)) {
                begin.isRepeatableRead = true;
                tokenizer.pop();
                if(!"".equals(tokenizer.peek())) {
                    throw ErrorItem.InvalidCommandException;
                }
                return begin;
            } else {
                throw ErrorItem.InvalidCommandException;
            }
        } else {
            throw ErrorItem.InvalidCommandException;
        }
    }


    private static boolean isType(String fieldType) {
        return ("int32".equals(fieldType) || "int64".equals(fieldType) ||
                "string".equals(fieldType));
    }

    private static boolean isName(String name) {
        return !(name.length() == 1 && !Tokenizer.isAlphaBeta(name.getBytes()[0]));
    }

    private static boolean isCmpOp(String op) {
        return ("=".equals(op) || ">".equals(op) || "<".equals(op));
    }

    private static boolean isLogicOp(String op) {
        return ("and".equals(op) || "or".equals(op));
    }

}
