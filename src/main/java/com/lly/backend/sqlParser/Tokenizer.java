package com.lly.backend.sqlParser;


import com.lly.common.ErrorItem;

/**
 *sql语句解析器,将语句切割成多个 token。对外提供了 peek()、pop() 方法方便取出 Token 进行解析
 */
public class Tokenizer {
    private byte[] statement;
    private int pos;
    private String currentToken;
    private boolean flushToken;
    private Exception err;

    public Tokenizer(byte[] statement) {
        this.statement = statement;
        this.pos = 0;
        this.currentToken = "";
        this.flushToken = true;
    }

    public String peek() throws Exception {
        if(err != null) {
            throw err;
        }
        if(flushToken) {
            String token = null;
            try {
                token = next();
            } catch(Exception e) {
                err = e;
                throw e;
            }
            currentToken = token;
            flushToken = false;
        }
        return currentToken;
    }

    public void pop() {
        flushToken = true;
    }
    
    private String next() throws Exception {
        if(err != null) {
            throw err;
        }
        return nextMetaState();
    }

    private String nextMetaState() throws Exception {
        // 跳过空白字符和判断是否到达末尾
        while(true) {
            Byte b = peekByte(); // 查看当前字节
            if(b == null) { // 如果到达字节数组末尾，返回空字符串
                return "";
            }
            if(!isBlank(b)) { // 如果当前字节不是空白字符，跳出循环
                break;
            }
            popByte(); // 跳过空白字符
        }

        byte b = peekByte(); // 再次查看当前字节
        if(isSymbol(b)) { // 如果是符号字符
            popByte();
            return new String(new byte[]{b}); // 返回符号作为 token
        } else if(b == '"' || b == '\'') { // 如果是引号字符
            return nextQuoteState();
        } else if(isAlphaBeta(b) || isDigit(b)) { // 如果是字母或数字字符
            return nextTokenState();
        } else { // 其他字符
            err = ErrorItem.InvalidCommandException;
            throw err;
        }
    }

    /**
     * 处理字母数字开头的 token, 关键字和标识符
     * @return 关键词或标识符
     */
    private String nextTokenState() {
        StringBuilder sb = new StringBuilder();
        while(true) {
            Byte b = peekByte();
            if(b == null || !(isAlphaBeta(b) || isDigit(b) || b == '_')) {
                if(b != null && isBlank(b)) {
                    popByte();
                }
                return sb.toString();
            }
            sb.append(new String(new byte[]{b}));
            popByte();
        }
    }

    /**
     * 处理引号开头的 token，都是字符串
     * @return 引号内的字符串
     */
    private String nextQuoteState() throws Exception {
        byte quote = peekByte(); // 获取当前引号字符
        popByte(); // 前进一位
        StringBuilder sb = new StringBuilder();
        while(true) {
            Byte b = peekByte(); // 获取下一个字符
            if(b == null) {
                err = ErrorItem.InvalidCommandException;
                throw err;
            }
            if(b == quote) {
                popByte(); // 如果遇到匹配的引号，结束循环
                break;
            }
            sb.append(new String(new byte[]{b}));
            popByte(); // 前进一位
        }
        return sb.toString();
    }

    /**
     * 前进一位
     */
    private void popByte() {
        pos ++;
        if(pos > statement.length) {
            pos = statement.length;
        }
    }


    /**
     * 查看当前字节
     * @return 当前字节
     */
    private Byte peekByte() {
        if(pos == statement.length) {
            return null;
        }
        return statement[pos];
    }


    static boolean isSymbol(byte b) {
        return (b == '>' || b == '<' || b == '=' || b == '*' ||
                b == ',' || b == '(' || b == ')');
    }

    static boolean isBlank(byte b) {
        return (b == '\n' || b == ' ' || b == '\t');
    }
    static boolean isDigit(byte b) {
        return (b >= '0' && b <= '9');
    }

    static boolean isAlphaBeta(byte b) {
        return ((b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z'));
    }


    public byte[] errStat() {
        byte[] res = new byte[statement.length+3];
        System.arraycopy(statement, 0, res, 0, pos);
        System.arraycopy("<< ".getBytes(), 0, res, pos, 3);
        System.arraycopy(statement, pos, res, pos+3, statement.length-pos);
        return res;
    }
}
