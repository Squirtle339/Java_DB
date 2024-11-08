package com.lly.backend.DM.logger;

import com.lly.common.ErrorItem;
import com.lly.common.utils.Error;
import com.lly.common.utils.Parser;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.primitives.Bytes;

public class LoggerImpl implements Logger{

    // 随机数种子防止hash碰撞
    private static final int SEED = 13331;
    
    public static final String LOG_SUFFIX = ".log";

    private static final int OF_SIZE = 0;
    private static final int OF_CHECKSUM = OF_SIZE + 4;
    private static final int OF_DATA = OF_CHECKSUM + 4;

    private RandomAccessFile file;
    private FileChannel fc;
    private Lock lock;

    private long position;  // 当前日志指针的位置
    private long fileSize;  // 初始化时记录，log操作不更新
    private int xChecksum;

    LoggerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        lock = new ReentrantLock();
        init();
    }
    LoggerImpl(RandomAccessFile raf, FileChannel fc, int xChecksum) {
        this.file = raf;
        this.fc = fc;
        this.xChecksum = xChecksum;
        lock = new ReentrantLock();
    }

    /*
     * 打开一个日志文件做的初始化工作，检查校验和，处理尾部坏日志
     */
    private void init() {
        long fileSize = 0;
        try {
            fileSize = fc.size();
        } catch (Exception e) {
            Error.error(e);
        }
        if (fileSize < 4) {
            Error.error(ErrorItem.BadLogFileException);
        }
        // 读取校验和
        ByteBuffer raw = ByteBuffer.allocate(4);
        try {
            fc.read(raw, 0);
        } catch (Exception e) {
            Error.error(e);
        }
        int xChecksum = Parser.getInt(raw.array());
        this.fileSize = fileSize;
        this.xChecksum = xChecksum;

        checkAndRemoveTail();

    }

    // 检查并移除bad tail
    private void checkAndRemoveTail() {
        //指针回到第一条日志的起点
        rewind();
        int xCheck = 0;
        // 遍历每一条日志，计算Checksum，并求和得到XChecksum
        while(true) {
            byte[] log = internNext();
            if(log == null) break;
            xCheck = calChecksum(xCheck, log);
        }
        if (xCheck!=xChecksum){
            Error.error(ErrorItem.BadLogFileException);
        }
        // 截断文件到正常日志的末尾
        try {
            truncate(position);
        } catch (Exception e) {
            Error.error(e);
        }
        //移动文件指针到文件末尾
        try {
            file.seek(position);
        }
        catch (Exception e) {
            Error.error(e);
        }
        rewind();
    }

    /*
     * 读取下一条日志,如果校验和错误，返回null
     */
    private byte[] internNext() {
        // 如果当前位置+日志长度大于文件大小，代表已经读完了
        if(position + OF_DATA >= fileSize) {
            return null;
        }
        ByteBuffer tmp = ByteBuffer.allocate(4);
        try{
            fc.position(position);
            fc.read(tmp);
        } catch (Exception e) {
            Error.error(e);
        }
        int size=Parser.getInt(tmp.array());
        //filesize小代表是数据库崩溃时没写完，这一条日志是BadTail
        if(position + size + OF_DATA > fileSize) {
            return null;
        }
        //读取一条日志
        ByteBuffer buf = ByteBuffer.allocate(OF_DATA + size);
        try {
            fc.position(position);
            fc.read(buf);
        } catch (Exception e) {
            Error.error(e);
        }
        byte[] log = buf.array();
        // 计算校验和
        int checkSum1 = calChecksum(0, Arrays.copyOfRange(log, OF_DATA, log.length));
        // 读取校验和
        int checkSum2 = Parser.getInt(Arrays.copyOfRange(log, OF_CHECKSUM, OF_DATA));
        if (checkSum1 != checkSum2) {
            return null;
        }
        position += log.length;
        return log;
    }

    /*
     * 计算校验和
     * @param xCheck 上一次的校验和
     */
    private int calChecksum(int xCheck, byte[] log) {
        for (byte b : log) {
            xCheck = xCheck * SEED + b;
        }
        return xCheck;
    }

    /*
     * 写入一条日志,先将数据包装为日志格式，再写入文件，再更新文件的校验和，最后强制写入磁盘
     */
    @Override
    public void log(byte[] data) {
        byte[] log = wrapLog(data);
        ByteBuffer buf = ByteBuffer.wrap(log);
        this.xChecksum = calChecksum(this.xChecksum, log);
        lock.lock();
        try {
            //写入日志
            fc.write(buf, position);
            //更新校验和
            fc.write(ByteBuffer.wrap(Parser.int2Byte(xChecksum)), 0);
            //写入磁盘
            fc.force(false);
        }catch (Exception e) {
            Error.error(e);
        }finally {
            lock.unlock();
        }
    }

    /*
     * 把数据包装为日志标准格式[Size][Checksum][Data]，加上校验和
     */
    private byte[] wrapLog(byte[] data) {
        byte[] checksum = Parser.int2Byte(calChecksum(0, data));
        byte[] size = Parser.int2Byte(data.length);
        return Bytes.concat(size, checksum, data);

    }

    @Override
    public void truncate(long x) throws Exception {
        lock.lock();
        try {
            fc.truncate(x);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public byte[] next() {
        lock.lock();
        try {
            byte[] log = internNext();
            if(log == null) return null;
            //只取Data部分
            return Arrays.copyOfRange(log, OF_DATA, log.length);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void rewind() {
        position = 4;
    }

    @Override
    public void close() {
        try {
            fc.close();
            file.close();
        } catch(IOException e) {
            Error.error(e);
        }
    }
}
