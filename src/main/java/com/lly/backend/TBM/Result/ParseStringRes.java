package com.lly.backend.TBM.Result;

public class ParseStringRes {
    public String str;
    /**
     * 下一个字符串的开始位置
     */
    public int next;

    public ParseStringRes(String str, int next) {
        this.str = str;
        this.next = next;
    }
}
