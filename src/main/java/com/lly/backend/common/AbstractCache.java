package com.lly.backend.common;


import com.lly.common.ErrorItem;

import java.util.HashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * AbstractCache 实现了一个引用计数策略的缓存
 */
public abstract class AbstractCache<T> {


    private HashMap<Long, T> cache;    // 缓存数据
    private HashMap<Long, Integer> references;    // 引用计数
    private HashMap<Long, Boolean> getting;        // 记录资源是否正在从数据源获取中（从数据源获取资源是一个相对费时的操作）

    private int maxResource;    // 缓存的最大缓存资源数
    private int count = 0;    // 缓存中元素的个数
    private Lock lock;    // 用于保护缓存的锁

    public AbstractCache(int maxResource) {
        this.cache = new HashMap<>();
        this.references = new HashMap<>();
        this.getting = new HashMap<>();
        this.maxResource = maxResource;
        this.lock = new ReentrantLock();
    }

    /**
     * 获取资源
     * @param key 资源的键
     * @return 资源
     * @throws Exception
     */
    protected T get(long key) throws Exception {
        while(true){
            lock.lock();//缓存加锁
            // 请求的资源正在被其他线程获取，释放锁，等待1ms，继续while下一个循环
            if(getting.containsKey(key)){
                lock.unlock();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
                continue;
            }
            // 资源在缓存中，直接返回，引用计数加1
            if(cache.containsKey(key)){
                T obj = cache.get(key);
                references.put(key, references.get(key) + 1);
                lock.unlock();
                return obj;
            }
            else {
                //缓存已满，抛出异常
                if(maxResource > 0 && count == maxResource) {
                    lock.unlock();
                    throw ErrorItem.CacheFullException;
                }
                //准备去获取资源
                count ++;
                getting.put(key, true);
                lock.unlock();
                break;
            }
        }
        //开始获取资源
        T obj = null;
        try {
            obj = getForCache(key);
        } catch (Exception e) {
            //获取资源失败，取消获取，抛出异常
            lock.lock();
            count --;
            getting.remove(key);
            lock.unlock();
            throw e;
        }
        //获取资源成功，更新缓存
        lock.lock();
        getting.remove(key);
        cache.put(key, obj);
        references.put(key, 1);
        lock.unlock();
        return obj;
    }
    /**
     * 释放一个缓存资源的引用
     * @param key 资源的键
     */
    protected void release(long key) {
        lock.lock();
        try{
            //引用计数减1
            int ref = references.get(key) - 1;
            //引用计数为0，释放资源
            if(ref == 0){
                //写回资源
                T obj = cache.get(key);
                releaseForCache(obj);
                //删除缓存相关数据
                cache.remove(key);
                references.remove(key);
                count --;
            }
            else{
                //引用计数减1
                references.put(key, ref);
            }
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * 关闭缓存，写回所有资源
     */
    protected void close() {
        lock.lock();
        try{
            for(long key: cache.keySet()){
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
            }
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * 当资源不在缓存时的获取行为
     */
    protected abstract T getForCache(long key) throws Exception;
    /**
     * 当资源被驱逐时的写回行为
     */
    protected abstract void releaseForCache(T obj);
}
