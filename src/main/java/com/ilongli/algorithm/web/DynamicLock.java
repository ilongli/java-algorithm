package com.ilongli.algorithm.web;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 动态锁（根据指定变量参数进行加锁）
 *
 * <p>
 * <br>场景：<br>
 * 处理的业务有加锁要求，并且是对某一字段进行归类加锁。<br>
 * 比如用户请求某一接口，该接口业务要求加锁，并对相同的用户名进行加锁操作。
 * <p>
 *
 * <p>
 * <br>说明：<br>
 * 对某一字段进行加锁，是对每个字段独立new一个lock而实现的。<br>
 * 考虑该字段数量很大，大量的new Lock()容易造成内存溢出，故规定一个容量(capacity)，对字段平均分布到[0, capacity]内。
 * <p>
 *
 * @author ilongli
 * @date 2020/7/4 16:44
 */
public class DynamicLock {

    private final Logger LOGGER = LoggerFactory.getLogger(DynamicLock.class);

    // 锁的最大容量(默认为128)
    private int capacity = 128;

    // 存放锁的Map(要求线性安全)
    private final ConcurrentHashMap<Integer, ReentrantLock> lockMap;

    // 业务
    private final Work work;

    /**
     * 构造函数
     *
     * @param capacity 容量
     * @param work     自定义业务
     */
    public DynamicLock(int capacity, Work work) {
        this.capacity = capacity;
        this.lockMap = new ConcurrentHashMap<>(capacity);
        this.work = work;
    }

    /**
     * 处理业务
     *
     * @param obj 根据这个obj加锁
     */
    public void handle(Object obj) {
        // 得到obj的哈希值
        int hashCode = Math.abs(obj.hashCode());

        // 取模，得到坐标
        int position = hashCode % this.capacity;
        LOGGER.debug("[{}] - the position is: {}", obj, position);

        // 根据position坐标获取对应的锁(如果没有，就new一个新锁)
        ReentrantLock lock = this.lockMap.computeIfAbsent(position, k -> new ReentrantLock());

        LOGGER.debug("{} - in and try to get the lock", obj);

        // 加锁
        lock.lock();

        LOGGER.debug("{} - get the lock and doing work", obj);

        // 业务
        this.work.apply();

        LOGGER.debug("{} - finish and unlock", obj);

        // 让锁
        lock.unlock();

    }

    interface Work {
        void apply();
    }
}
