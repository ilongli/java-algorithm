package com.ilongli.algorithm.web;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 延迟消息队列（消息聚合延迟推送）
 * <p>
 * 场景：
 * 频繁的接口请求，如人脸摄像头识别到人脸的数据推送、频繁的数据采集等。
 * 接口接收到数据后需要交给下游业务处理，接口与下游业务通过MQ(如ActiveMQ或Kafka)交互数据，保证高可用。
 * 下游业务每次对数据的处理时间较长或开销较大，如数据库的操作。
 * 因此尽量让数据聚合到一个队列，下游业务统一处理，批量操作。
 * <p>
 * 流程：
 * 1)上游接口接收消息，将消息聚合
 * 2)在适当的时候(队列满了，到达轮询的时间)将消息推送到MQ
 * 3)下游业务从MQ中读取队列，统一处理，批量操作
 * <p>
 * 算法涉及到步骤1，如何将消息聚合，平滑推送（大量数据时，队列满后推送；空闲时，推送间隔固定）
 * <p>
 * 算法效果：
 * 1)定时器每隔N秒后自动将消息队列的数据pollAll
 * 2)消息队列满后自动pollAll
 * 3)定时器每隔一段时间会自动将消息队列的数据pollAll
 * 4)消息队列满后会让定时器“重新计时”，即队列满后隔N秒才会进入下一次pollAll操作，以达到平滑过渡效果
 * 5)不使用将定时器cancel()然后重新schedule()的方法达到“重新计时”的效果(内存和CPU开销过大？)，仅靠算法实现
 *
 * @author ilongli
 * @date 2020/7/4 10:37
 */
public class DelayedMessageQueue {

    private final Logger LOGGER = LoggerFactory.getLogger(DelayedMessageQueue.class);

    // 定时器间隔(默认5000毫秒)
    private long intervalMillis = 5000L;

    // 消息队列的阈值(上限)
    private int msgThreshold = 20;

    // 消息队列，不定时不定量地往queue中放入数据，并发操作，需要线性安全
    private LinkedBlockingQueue<Object> queue;

    // 消息队列pollAll后的操作
    AfterPollAll afterPollAll;

    /**
     * 构造方法
     * @param msgThreshold  队列上限
     * @param afterPollAll  pollAll后操作
     */
    public DelayedMessageQueue(int msgThreshold, AfterPollAll afterPollAll) {
        this.msgThreshold = msgThreshold;
        queue = new LinkedBlockingQueue<>(msgThreshold);
        this.afterPollAll = afterPollAll;
        this.start();
    }
    /**
     * 构造方法
     * @param msgThreshold  队列上限
     * @param intervalMillis    定时器间隔(毫秒)
     * @param afterPollAll  pollAll后操作
     */
    public DelayedMessageQueue(int msgThreshold, long intervalMillis, AfterPollAll afterPollAll) {
        this.msgThreshold = msgThreshold;
        this.intervalMillis = intervalMillis;
        queue = new LinkedBlockingQueue<>(msgThreshold);
        this.afterPollAll = afterPollAll;
        this.start();
    }

    // 定时器的等待队列，实现平滑过渡的关键(需要线性安全？)
    private final LinkedBlockingQueue<Long> waitQueue = new LinkedBlockingQueue<>();

    // 用于记录下一次休眠的结束时间，和waitQueue配合使用
    private final AtomicLong nextSleepTime = new AtomicLong();

    ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;
    ScheduledFuture<?> scheduledFuture;

    /**
     * 开启定时器
     */
    private void start() {
        // stop if running
        if (this.scheduledThreadPoolExecutor != null) {
            stop();
        }

        this.scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(1);
        // scheduleWithFixedDelay方法以实现定时任务之间固定间隔N秒
        this.scheduledFuture = this.scheduledThreadPoolExecutor.scheduleWithFixedDelay(() -> {
            LOGGER.debug("timer in!!!");
            try {
                // 将等待队列里面的时间全部休眠完毕，才算真正进入“定时器”
                while (!waitQueue.isEmpty()) {
                    //noinspection BusyWait
                    Thread.sleep(waitQueue.poll());
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            // pollAll操作
            pollAll(true);
        }, this.intervalMillis, this.intervalMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * 关闭定时器
     */
    private void stop() {
        this.scheduledFuture.cancel(false);
        this.scheduledThreadPoolExecutor.shutdown();
        this.scheduledThreadPoolExecutor = null;
    }


    /**
     * 处理消息队列
     */
    private void pollAll(boolean isTimer) {
        ArrayList<Object> objects = new ArrayList<>();
        queue.drainTo(objects);
//        while (queue.size() != 0) {
//            objects.add(queue.poll());
//        }
        LOGGER.debug("获取到的元素[" + (isTimer ? "timer" : "max") + "]：" + objects);

        afterPollAll.apply(objects);
    }


    /**
     * 放入数据
     *
     * @param object 要放入的数据
     * @throws InterruptedException 中断异常
     */
    public void putMsg(Object object) throws InterruptedException {
        this.queue.put(object);


        // 当消息达到指定的阈值，自动pollAll
        if (queue.size() >= this.msgThreshold) {

            // pollAll操作
            pollAll(false);

            long remain = this.scheduledFuture.getDelay(TimeUnit.MILLISECONDS);
            LOGGER.debug("remain:" + remain);


            long cur = System.currentTimeMillis();
            // 这里分两种情况
            if (remain > 0) {
                // 1)remain>0，下一次定时还没有到达
                // 清空sleep队列
                this.waitQueue.clear();
                // 放入最新的sleep值
                this.waitQueue.put(this.intervalMillis - remain);
            } else {
                // 2)remain<0，定时器已经到达，但是正在sleep
                // 下一次sleep结束时间 - 当前时间 = 剩余的sleep时间
                long sleepRemain = this.nextSleepTime.get() - cur;
                // 将sleep时间补到INTERVAL_MILLIS，添加延迟到延迟队列
                this.waitQueue.put(this.intervalMillis - sleepRemain);
            }
            // 更新下一次sleep的结束时间
            this.nextSleepTime.set(cur + this.intervalMillis);
        }
    }

    /**
     * 消息队列pollAll后的用户自定义操作
     */
    interface AfterPollAll {
        void apply(ArrayList<Object> objects);
    }
}
