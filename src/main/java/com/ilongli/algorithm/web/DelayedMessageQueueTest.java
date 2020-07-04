package com.ilongli.algorithm.web;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Scanner;

/**
 * 延迟消息队列（消息聚合延迟推送）<br>
 *  The Test of {@link DelayedMessageQueue}
 * @author ilongli
 * @date 2020/7/4 16:08
 */
public class DelayedMessageQueueTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(DelayedMessageQueueTest.class);

    public static void main(String[] args) throws InterruptedException {

        DelayedMessageQueue delayedMessageQueue = new DelayedMessageQueue(5, 10000, objects -> {
            LOGGER.debug("将队列{}放入MQ，交由下游业务处理", objects);
        });

        // 控制台输入内容，模拟消息放入队列的操作
        while (true) {
            // 获取用户输入，模拟请求接口，发送消息
            Scanner scanner = new Scanner(System.in);
            String object = scanner.next();
            delayedMessageQueue.putMsg(object);
        }

    }


}
