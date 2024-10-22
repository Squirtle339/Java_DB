package com.lly.common.utils;

import java.security.SecureRandom;
import java.util.Random;

public class RandomUtils {
    public static byte[] randomBytes(int lenVc) {
        Random r = new SecureRandom();
        byte[] buf = new byte[lenVc];
        r.nextBytes(buf);
        return buf;
    }
}
