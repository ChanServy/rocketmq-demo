package com.chan.mq.rocketmq.base.producer;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

/**
 * @author CHAN
 * @apiNote 发送同步消息
 * @since 2022/3/19
 */
@Slf4j
public class SyncProducer {
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
            Message message = new Message("base", "tag1", ("Hello World" + i).getBytes());
            // 5.发送同步消息
            SendResult result = producer.send(message);
            // 发送状态
            // SendStatus status = result.getSendStatus();
            // 消息ID
            // String msgId = result.getMsgId();
            // 消息接收队列ID
            // int queueId = result.getMessageQueue().getQueueId();
            log.debug("SendResult：{}", result);
        }
        // 6.关闭生产者producer
        producer.shutdown();
    }
}
