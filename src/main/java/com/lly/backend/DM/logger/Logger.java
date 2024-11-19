package com.lly.backend.DM.logger;

import com.lly.common.ErrorItem;
import com.lly.common.utils.Error;
import com.lly.common.utils.Parser;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public interface Logger {
    void log(byte[] data);
    void truncate(long x) throws Exception;
    byte[] next();
    void rewind();
    void close();

    public static Logger create(String path) {
        File f = new File(path+LoggerImpl.LOG_SUFFIX);
        System.out.println("log file path: " + f.getAbsolutePath());
        try {
            if(!f.createNewFile()) {
                Error.error(ErrorItem.FileExistsException);
            }
        } catch (Exception e) {
            Error.error(e);
        }
        if(!f.canRead() || !f.canWrite()) {
           Error.error(ErrorItem.FileCannotRWException);
        }
        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Error.error(e);
        }
        ByteBuffer buf = ByteBuffer.wrap(Parser.int2Byte(0));
        try {
            fc.position(0);
            fc.write(buf);
            fc.force(false);
        } catch (Exception e) {
            Error.error(e);
        }

        return new LoggerImpl(raf, fc, 0);
    }

    public static Logger open(String path) {
        File f = new File(path+LoggerImpl.LOG_SUFFIX);
        System.out.println("log file path: " + f.getAbsolutePath());
        if (!f.exists()) {
            Error.error(ErrorItem.FileNotExistsException);
        }
        if (!f.canRead() || !f.canWrite()) {
            Error.error(ErrorItem.FileCannotRWException);
        }
        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Error.error(e);
        }
        return new LoggerImpl(raf, fc);


    }
}
