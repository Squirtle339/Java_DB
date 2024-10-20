# Java_DB

手写一个简易版关系型数据库，参考了[CN-GuoZiyang/MYDB: 一个简单的数据库实现 (github.com)](https://github.com/CN-GuoZiyang/MYDB)



本文记录了实现过程中的一些关键点以及自己的思考



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

------



### DM



#### 一、基础缓存框架

❓原作者为什么选择了计数缓存而不是常见的LRU缓存？

作者提到当缓存满了时，LRU 会自动驱逐最久未被访问的资源，而上层模块确不能感知到，只有记录下资源的最后修改时间，并且让缓存记录下资源被驱逐的时间，于是想简化为计数策略通过增加一个 `release(key)` 方法，要求上层模块在不再使用某个资源时显式释放对该资源的引用。

所以个人觉得作者只是提出了一个简化版的缓存框架，也存在一些问题：在缓存满了之后无法自动释放缓存，此时可能会直接报错（类似于 JVM 的 OOM 错误）。但计数缓存也有一定的优点：在某些高精度的系统中，如嵌入式系统或对数据一致性要求极高的分布式系统，引用计数缓存能够防止意外驱逐关键数据，提供更高的控制和灵活性。

🚩查阅资料，现代数据库系统最常用的缓存方案依然是 **LRU** 或其改进版本，如 **LRU-K** 和 **ARC**，所以如果有时间，**完成项目后还是希望自己能优化实现一个更常用的缓存框架**（虽然好像有点难😵）



#### 二、共享内存数组（不得已而为之）

因为java的数组在内存中也是对象，无法实现子数组和原数组共享内存，于是原作者自己实现一个mySubArray类实现共享内存（巧妙！）

通过*ByteBuffer*也可以实现，创建一个指向数组内部的缓冲区视图，因此对其进行的修改将会影响原数组，但是好像数组（现在是buffer）操作就和潜意识中有区别了，还是原作者这个方法更容易让人理解接受

```java
public class SubArray {
    public byte[] raw;
    public int start;
    public int end;
    
    public SubArray(byte[] raw, int start, int end) {
        this.raw = raw;
        this.start = start;
        this.end = end;
    }
}
```



#### 三、数据页缓存

> **DM 将文件系统抽象成页面**，每次对文件系统的读写都是以页面为单位的。同样，从文件系统读进来的数据也是以页面为单位进行缓存的