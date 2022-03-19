package com.chan.mq.rocketmq.base.producer;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.util.concurrent.TimeUnit;

/**
 * @author CHAN
 * @apiNote 发送异步消息
 * @since 2022/3/19
 */
@Slf4j
public class AsyncProducer {
    @SneakyThrows
    public static void main(String[] args) {
        // 1.创建消息生产者producer，并制定生产者组名
        DefaultMQProducer producer = new DefaultMQProducer("group1");
        // 2.指定Nameserver地址
        producer.setNamesrvAddr("192.168.1.102:9876;192.168.1.107:9876");
        // 3.启动producer
        producer.start();
        for (int i = 0; i < 10; i++) {
            // 4.创建消息对象，指定主题Topic、Tag和消息体
            Message message = new Message("base", "tag2", ("Hello World" + i).getBytes());
            // 5.发送异步消息
            producer.send(message, new SendCallback() {
                /**
                 * 发送成功的回调函数
                 * @param sendResult SendResult
                 */
                @Override
                public void onSuccess(SendResult sendResult) {
                    log.debug("发送结果：{}", sendResult);
                }

                /**
                 * 发送失败的回调函数
                 * @param throwable Throwable
                 */
                @Override
                public void onException(Throwable throwable) {
                    log.error("发送异常：", throwable);
                }
            });
        }
        TimeUnit.SECONDS.sleep(1);// 让主线程睡一会，方便回调函数返回结果执行完成。直接关闭生产者会报错，因为可能异步执行未完成。
        // 6.关闭生产者producer
        producer.shutdown();
    }
}
