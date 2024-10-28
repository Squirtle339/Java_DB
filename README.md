

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



实现了一个抽象基础缓存框架类***AbstractCache<T>***，包含如下数据结构

```java
private HashMap<Long, T> cache;    // 缓存数据
private HashMap<Long, Integer> references;    // 引用计数
private HashMap<Long, Boolean> getting;        // 记录资源是否正在从数据源获取中（从数据源获取资源是一个相对费时的操作）
private int maxResource;    // 缓存的最大缓存资源数
private int count = 0;    // 缓存中元素的个数
private Lock lock;    // 用于保护缓存的锁
```

```java


    //获取资源
    protected T get(long key) throws Exception {
//		...
    }
 	//释放一个缓存资源
    protected void release(long key) {
//		...
    }
	//关闭缓存，写回所有资源
    protected void close() {
//        ...
    }

	/*两个抽象方法*/
    //当资源不在缓存时的获取行为
    protected abstract T getForCache(long key) throws Exception;
    //当资源被驱逐时的写回行为
    protected abstract void releaseForCache(T obj);
```



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

#### 三、数据页类

一个页面类代表一个页面，包含页面数据和其他信息，参考大部分数据库的设计，将默认数据页大小定为 8K

```java
public class PageImpl implements Page{
    private int pageNumber;		//这个页面的页号
    private byte[] data;		//这个页面的数据
    private boolean isdirty;    //在缓存驱逐的时候，脏页面需要被写回磁盘
    private Lock lock;
    private PageCache pageCache; //表示页面缓存类
 }
```

页面类提供如下接口，实现比较简单：

```java
void lock();
void unlock();
void release();
void setDirty(boolean dirty);
boolean isDirty();
int getPageNumber();
byte[] getData();
```

#### 四、数据页缓存

> **DM 将文件系统抽象成页面**，每次对文件系统的读写都是以页面为单位的。同样，从文件系统读进来的数据也是以页面为单位进行缓存的

页面缓存基于基础缓存框架实现，除了实现两个抽象方法外，还实现了一些基本的页面缓存操作

```java
int newPage(byte[] initData);			//在缓存新建一个空白页，返回页号，立即写入数据库
Page getPage(int pgno) throws Exception;//根据页号获取页对象，分在和不在缓存的情况
void close();							//关闭缓存，写回所有资源
void release(Page page);				//释放一个缓存页面
void truncateByPgno(int maxPgno);		//根据页号截断数据库文件
int getPageNumber();					//获取页总数
void flushPage(Page pg);				//将页数据写回到数据库文件中
```



🎈一个小知识点:

对于缓存里记录的总页面数，使用AtomicInteger实现，不需要额外的锁来实现同步，比较方便

```java
//在多线程环境下，无需额外的同步措施，即可保证操作的线程安全性
private AtomicInteger pageNumbers;

//AtomicInteger的一些常用操作
get();			 	//获取当前值。
set(int newValue);	//设置当前值。
incrementAndGet();	//原子地将当前值加一，并返回新值。
decrementAndGet();	//原子地将当前值减一，并返回新值。
addAndGet(int delta);//原子地将当前值加上给定的数值，并返回新值。
compareAndSet(int expect, int update);//如果当前值等于 expect，则原子地将该值设置为 update，并返回 true；否则，返回 false。

```



#### 五、页面管理

##### 首页

数据库文件的第一页，通常用作一些特殊用途，比如存储一些元数据，用来启动检查什么的

在这设计第一页用于启动检查，具体的原理是，在每次数据库启动时，会生成一串随机字节，存储在 100 ~ 107 字节。在数据库正常关闭时，会将这串字节，拷贝到第一页的 108 ~ 115 字节。

这样数据库在每次启动时，就会检查第一页两处的字节是否相同，以此来判断上一次是否正常关闭。如果是异常关闭，就需要执行数据的恢复流程。

❓为什么是100开始，原作者没解释，看看后面有没有伏笔吧

##### 普通页

结构为: [0:1] [2:8191]

其中前两位是偏移量，后面为数据，偏移量表示这一页的空闲位置的偏移，只需要一个short类型数就可以记录



**核心操作就是在普通页中写入数据，原理简单，基于offset进行System.arraycopy就行**

❓竟然没有页面数据的读取？？神奇



#### 六、日志文件

日志的二进制文件格式为：$$[XChecksum] [Log1] [log2]...[LogN] [BadTail]$$

- XChecksum 是一个四字节的整数，是对后续所有日志计算的校验和。
- Log1 ~ LogN 是常规的日志数据
  - 每条日志的格式为：$$[Size][Checksum][Data]$$
    - Size 是一个四字节整数，标识了 Data 段的字节数。
- BadTail 是在数据库崩溃时，没有来得及写完的日志数据，这个 BadTail 不一定存在。



🎈小知识点：

**校验和（Checksum）** 是一种用于验证数据完整性和准确性的技术，通过对数据内容进行特定的数学运算生成一个数值，与数据一起存储；当数据被读取时，数据库系统会重新计算该数据的校验和，并将其与存储的校验和进行比较，如果不同，则表明数据可能已损坏或被篡改。

缺点：无法防止故意篡改数据的攻击，所以通常会结合其他安全措施（如哈希算法或数字签名）【自己实现一下数字签名🚩】

##### 校验

在打开一个日志文件时，需要首先校验日志文件的 XChecksum，并移除文件尾部可能存在的 BadTail，由于 BadTail 该条日志尚未写入完成，文件的校验和也就不会包含该日志的校验和，去掉 BadTail 即可保证日志文件的一致性。





```java
private void checkAndRemoveTail() {
	//指针指向第一条日志起点
    rewind();
    int xCheck = 0;
    //遍历每一条日志，累加日志的校验和
    while(true) {
        byte[] log = internNext();
        if(log == null) break;
        xCheck = calChecksum(xCheck, log);
    }
    if(xCheck != xChecksum) {
        Panic.panic(Error.BadLogFileException);
    }
    // 截断文件到正常日志的末尾
    truncate(position);
    rewind();
}
```

🚩如何判断是一条BadTail呢？作者用的是这一个判断，应该是和写操作的各项顺序有关，等待后续写到了解答

```java
if(position + size + OF_DATA > fileSize) {
    return null;
}
```

##### 日志读

Logger 被实现成迭代器模式，通过 next() 方法，不断地从文件中读取下一条日志，next()方法的实现主要依靠internNext()，在读取每条日志时都会检查校验和

##### 日志写

写操作先将数据包装为日志格式，再写入文件，再更新文件的校验和，最后强制写入磁盘

```java
@Override
public void log(byte[] data) {
    byte[] log = wrapLog(data);//包装为日志格式
    ByteBuffer buf = ByteBuffer.wrap(log);
    this.xChecksum = calChecksum(this.xChecksum, log);
    lock.lock();
    try {
        fc.write(buf, position);        //写入日志
        fc.write(ByteBuffer.wrap(Parser.int2Byte(xChecksum)), 0);//更新文件校验和
        fc.force(false);        //强制写入磁盘
    }catch (Exception e) {
        Error.error(e);
    }finally {
        lock.unlock();
    }
}
```