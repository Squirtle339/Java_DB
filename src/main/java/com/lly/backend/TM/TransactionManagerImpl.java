package com.lly.backend.TM;

import com.lly.common.ErrorItem;
import com.lly.common.utils.Error;
import com.lly.common.utils.Parser;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TransactionManagerImpl implements TransactionManager{

    // XID文件头长度
    static final int LEN_XID_HEADER_LENGTH = 8;
    // 每个事务的占用长度
    private static final int XID_FIELD_SIZE = 1;
    // 事务的三种状态
    private static final byte FIELD_TRAN_ACTIVE   = 0;
    private static final byte FIELD_TRAN_COMMITTED = 1;
    private static final byte FIELD_TRAN_ABORTED  = 2;
    // 超级事务，永远为commited状态
    public static final long SUPER_XID = 0;
    // XID 文件后缀
    static final String XID_SUFFIX = ".xid";


    private RandomAccessFile file; //使用RandomAccessFile存储事务信息，允许随机访问文件中的任何位置
    private FileChannel fileChannel;//FileChannel是一个连接到文件的通道，可以通过文件通道读写文件

    private long xidCounter;//事务计数器，用于生成事务ID
    private Lock counterLock;//使用ReentrantLock实现事务计数器的线程安全


    /**
     * 构造函数
     * @param raf: 把XID文件使用RandomAccessFile映射到内存中的一个文件
     * @param fileChannel：文件访问通道
     */
    TransactionManagerImpl(RandomAccessFile raf, FileChannel fileChannel) {
        this.file = raf;
        this.fileChannel = fileChannel;
        this.counterLock = new ReentrantLock();
        checkXIDCounter();
    }

    /**
     * 检查XID文件是否合法
     * 读取XID_FILE_HEADER中的xidcounter，根据它计算文件的理论长度，对比实际长度
     */
    private void checkXIDCounter(){
        Long fileLen = 0L;
        try {
            fileLen = file.length();
        } catch (IOException e) {
            Error.error(ErrorItem.BadXIDFileException);
        }
        // 头文件不完整
        if(fileLen < LEN_XID_HEADER_LENGTH){
            Error.error(ErrorItem.BadXIDFileException);
        }

        ByteBuffer buf = ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);
        try {
            fileChannel.position(0);
            fileChannel.read(buf);
        } catch (IOException e) {
            Error.error(ErrorItem.BadXIDFileException);
        }
        //头文件的8个字节存储xidCounter的值
        xidCounter= Parser.getLong(buf.array());
        //计算文件的理论长度，end指向下一个事务的开始位置，也就是文件的末尾
        long end= getXidPosition(xidCounter+1);
        if(end != fileLen){
            Error.error(ErrorItem.BadXIDFileException);
        }
    }

    /**
     * 根据事务xid取得其在xid文件中对应的位置
     * 事务 xid 在文件中的状态就存储在 (xid-1)+8 字节处，xid-1 是因为 xid 0（Super XID） 的状态不需要记录
     * @param xidCounter
     * @return 事务在文件中的字节位置序号
     */
    private long getXidPosition(long xidCounter) {
        return LEN_XID_HEADER_LENGTH + (xidCounter-1)*XID_FIELD_SIZE;
    }

    /**
     * 开启一个事务应该包含如下步骤：
     * 1. 获取事务的xid
     * 2. 在文件中把xid事务状态设置为active
     * 2. 更新XID Header为新的xidCounter的值
     * @return
     */
    @Override
    public long begin() {
        counterLock.lock();
        try {
            long xid = xidCounter + 1;
            updateXID(xid, FIELD_TRAN_ACTIVE);
            incrXIDCounter();
            return xid;
        } finally {
            counterLock.unlock();
        }
    }



    /**
     * 更新xid事务的状态为status
     * @param xid
     * @param status
     */
    private void updateXID(long xid, byte status) {
        //计算xid事务在文件中的位置
        long offset = getXidPosition(xid);
        byte[] tmp = new byte[XID_FIELD_SIZE];
        tmp[0] = status;
        ByteBuffer buf = ByteBuffer.wrap(tmp);
        //将状态写入文件指定位置
        try {
            fileChannel.position(offset);
            fileChannel.write(buf);
        } catch (IOException e) {
            Error.error(e);
        }
        try {
            //强制同步缓存内容到文件中，防止数据丢失
            fileChannel.force(false);
        } catch (IOException e) {
            Error.error(e);
        }

    }

    /**
     * 将XID加一，并更新XID Header
     */
    private void incrXIDCounter() {
        xidCounter++;
        ByteBuffer buf = ByteBuffer.wrap(Parser.long2Byte(xidCounter));
        //写入
        try {
            fileChannel.position(0);
            fileChannel.write(buf);
        } catch (IOException e) {
            Error.error(e);
        }
        //强制同步缓存内容到文件中，防止数据丢失
        try {
            fileChannel.force(false);
        } catch (IOException e) {
            Error.error(e);
        }
    }

    /**
     * 提交XID事务，更新xid事务的状态为committed
     * @param xid
     */
    @Override
    public void commit(long xid) {
        updateXID(xid, FIELD_TRAN_COMMITTED);

    }

    /**
     * 取消XID事务，更新xid事务的状态为aborted
     * @param xid
     */
    @Override
    public void abort(long xid) {
        updateXID(xid, FIELD_TRAN_ABORTED);
    }

    /**
     * 关闭TM
     */
    @Override
    public void close() {
        try {
            fileChannel.close();
            file.close();
        } catch (IOException e) {
            Error.error(e);
        }

    }


    @Override
    public boolean isAlive(long xid) {
        return checkXIDStatus(xid, FIELD_TRAN_ACTIVE);
    }


    @Override
    public boolean isCommitted(long xid) {
        return checkXIDStatus(xid, FIELD_TRAN_COMMITTED);
    }

    @Override
    public boolean isAborted(long xid) {
        return checkXIDStatus(xid, FIELD_TRAN_ABORTED);
    }

    /**
     * 检测XID事务是否处于status状态
     * @param xid
     * @param status
     * @return
     */
    private boolean checkXIDStatus(long xid, byte status) {
        if(xid == SUPER_XID) return false;

        long offset = getXidPosition(xid);
        ByteBuffer buf = ByteBuffer.wrap(new byte[XID_FIELD_SIZE]);
        try {
            fileChannel.position(offset);
            fileChannel.read(buf);
        } catch (IOException e) {
           Error.error(e);
        }
        return buf.array()[0] == status;
    }
}
