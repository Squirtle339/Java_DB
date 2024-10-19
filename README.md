# Java_DB

手写一个简易版关系型数据库，参考：[CN-GuoZiyang/MYDB: 一个简单的数据库实现 (github.com)](https://github.com/CN-GuoZiyang/MYDB)

## 总览

Java_DB分为后端和前端，前后端通过 socket 进行交互。

- 前端（客户端）的职责很单一，读取用户输入，并发送到后端执行，输出返回结果，并等待下一次输入。
- 后端则需要解析 SQL，如果是合法的 SQL，就尝试执行并返回结果。不包括解析器，MYDB 的后端划分为五个模块，每个模块都又一定的职责，通过接口向其依赖的模块提供方法。五个模块如下：
  - **事务管理器TM**：通过维护 XID 文件来维护事务的状态，并提供接口供其他模块来查询某个事务的状态。
  - **数据管理器DM**：直接管理数据库数据库文件和日志文件。DM 的主要职责有：
    - 分页管理 DB 文件，并进行缓存；
    - 管理日志文件，保证在发生错误时可以根据日志进行恢复；
    - 抽象 DB 文件为 DataItem 供上层模块使用，并提供缓存。
  - **版本管理器VM**：基于两段锁协议实现调度序列的可串行化、实现 MVCC 以消除读写阻塞、实现读提交和可重复读两种隔离级别。
  - **索引管理器IM**：基于 B+ 树的索引
  - **字段与表管理器TBM**：对字段和表进行管理，解析SQL并操作表

## 后端实现

### TM

事务管理器的核心是维护一个XID文件，存储每个事务的状态。每一个事务都有一个 XID，从 1 开始标号，并自增，**不可重复**

**并特殊规定 XID 0 是一个超级事务（Super Transaction）。当一些操作想在没有申请事务的情况下进行，那么可以将操作的 XID 设置为 0。XID 为 0 的事务的状态永远是 committed。**

XID文件的头部8字节（long）记录了事务个数，后面的每**1个字节**依次记录XID事务的状态。

事务 xid 在文件中的状态就存储在 (xid-1)+8 字节处，xid-1 是因为 xid 0（Super XID） 的状态不需要记录。



TM提供七个接口和两个静态方法

```java
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
    * 创建事务管理器
    * 1. 根据传入的路径新建一个XID文件，
    * 2. 建立RandomAccessFile和FileChannel
    * 3. 写入空XID文件头，即设置 xidCounter 为 0
     */
    public static TransactionManagerImpl create(String path) {
        //...
    }

    /*
    *根据已有的XID文件路径打开一个XID文件并建立TM
    * 1. 根据传入的路径打开XID文件，
    * 2. 建立RandomAccessFile和FileChannel
     */
    public static TransactionManagerImpl open(String path) {
    	//...
    }
```

