package com.lly.common.utils;

public class Error {
    public static void error(Exception err) {
        err.printStackTrace();
        System.exit(1);
    }
}
