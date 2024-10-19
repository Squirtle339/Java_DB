package com.lly.backend.TM;

import com.lly.common.ErrorItem;
import com.lly.common.utils.Error;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public interface TransactionManager {

    //开启事务
    long begin();

    //提交事务
    void commit(long xid);

    //取消事务
    void abort(long xid);

    //关闭事务
    void close();

    //查询事务是否为正在进行的事务
    boolean isAlive(long xid);

    //查询事务是否为已经提交的事务
    boolean isCommitted(long xid);

    //查询事务是否为已经取消的事务
    boolean isAborted(long xid);

    /*
    * 创建事务管理器,根据传入的路径新建一个XID文件，注意若文件已经存在则抛出异常
    * 再建立RandomAccessFile和FileChannel,并写入空XID文件头，即设置 xidCounter 为 0
     */
    public static TransactionManagerImpl create(String path) {
        File f = new File(path+TransactionManagerImpl.XID_SUFFIX);
        //检查文件是否已经成功创建
        try{
            if(!f.createNewFile()){
                Error.error(ErrorItem.FileExistsException);
            }
        }
        catch (Exception e){
            Error.error(e);
        }
        //检查文件是否可读写
        if(!f.canRead() || !f.canWrite()){
            Error.error(ErrorItem.FileCannotRWException);
        }
        //创建文件通道和随机访问文件
        FileChannel fc = null;
        RandomAccessFile raf = null;
        try{
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        }
        catch (Exception e){
            Error.error(e);
        }
        //写入空XID文件头
        ByteBuffer buf = ByteBuffer.wrap(new byte[TransactionManagerImpl.LEN_XID_HEADER_LENGTH]);
        try{
            fc.position(0);
            fc.write(buf);
        }
        catch (Exception e){
            Error.error(e);
        }
        return new TransactionManagerImpl(raf, fc);
    }

    /*
    *根据已有的XID文件路径打开一个XID文件并建立TM
     */
    public static TransactionManagerImpl open(String path) {
        File f = new File(path+TransactionManagerImpl.XID_SUFFIX);
        if(!f.exists()) {
            Error.error(ErrorItem.FileNotExistsException);
        }
        //检查文件是否可读写
        if(!f.canRead() || !f.canWrite()){
            Error.error(ErrorItem.FileCannotRWException);
        }
        //创建文件通道和随机访问文件
        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Error.error(e);
        }
        return new TransactionManagerImpl(raf, fc);
    }
}
