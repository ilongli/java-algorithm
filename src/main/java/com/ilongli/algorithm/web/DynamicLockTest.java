package com.ilongli.algorithm.web;

/**
 * 动态锁（根据指定变量参数进行加锁）
 * The Test of {@link DynamicLock}
 * @author ilongli
 * @date 2020/7/4 16:44
 */
public class DynamicLockTest {

    public static void main(String[] args) {

        // 自定义业务，这里等待5秒
        DynamicLock.Work work = () -> {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        };

        DynamicLock dynamicLock = new DynamicLock(128, work);

        /**
         * 测试效果：
         * Jack和Rose都请求了10次接口(线程均不一样)
         * Jack和Rose的接口会每个5秒完成(finish)一次
         */
        for (int i = 0; i < 10; i++) {
            new Thread(() -> {
                // 模拟Jack这个用户请求接口
                dynamicLock.handle("Jack");
            }).start();
            new Thread(() -> {
                // 模拟Rose这个用户请求接口
                dynamicLock.handle("Rose");
            }).start();
        }
    }


}
