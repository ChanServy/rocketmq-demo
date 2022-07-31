package com.chan.mq.rocketmq.base.producer;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.concurrent.TimeUnit;

/**
 * 发送异步消息：
 * 异步消息通常用在对响应时间敏感的业务场景，即发送端（producer）不能容忍长时间地等待Broker的响应。
 *
 * @author CHAN
 * @apiNote 发送异步消息
 * @since 2022/3/19
 */
@Slf4j
public class AsyncProducer {
    @SneakyThrows
    public static void main(String[] args) {
        // 1.实例化消息生产者Producer，并制定生产者组名
        DefaultMQProducer producer = new DefaultMQProducer("group_test02");
        // 2.指定Nameserver地址
        producer.setNamesrvAddr("localhost:9876");
        // 3.启动producer实例
        producer.start();
        producer.setRetryTimesWhenSendAsyncFailed(0);
        for (int i = 0; i < 10; i++) {
            // 4.创建消息对象，指定主题Topic、Tag和消息体
            Message message = new Message("TopicTest", "TagA", "OrderID188", ("Hello World" + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            // 5.发送异步消息，SendCallback接收异步返回结果的回调
            producer.send(message, new SendCallback() {
                /**
                 * 发送成功的回调函数
                 */
                @Override
                public void onSuccess(SendResult sendResult) {
                    String msgId = sendResult.getMsgId();
                    log.debug("发送结果：{}，消息ID：{}", sendResult, msgId);
                }

                /**
                 * 发送失败的回调函数
                 */
                @Override
                public void onException(Throwable throwable) {
                    log.error("发送异常：", throwable);
                }
            });
        }
        TimeUnit.SECONDS.sleep(1);// 让主线程睡一会再关闭producer，方便回调函数返回结果执行完成。直接关闭生产者会报错，因为可能异步执行未完成。
        // 6.如果不再发送消息，关闭Producer实例。
        producer.shutdown();
    }
}
