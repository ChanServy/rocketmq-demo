package com.chan.mq.rocketmq.base.producer;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * 发送同步消息：
 * 生产者发送消息，之后会同步等待 RocketMQ 服务器接收消息后返回一个结果，也就是发送状态，生产者接收到这个结果之后才继续执行。
 * 这种可靠性同步地发送方式使用的比较广泛，比如：重要的消息通知，短信通知。
 *
 * @author CHAN
 * @apiNote 发送同步消息
 * @since 2022/3/19
 */
@Slf4j
public class SyncProducer {
    @SneakyThrows
    public static void main(String[] args) {
        // 1.实例化消息生产者Producer，并制定生产者组名
        DefaultMQProducer producer = new DefaultMQProducer("group_test01");
        // 2.指定Nameserver地址
        producer.setNamesrvAddr("192.168.1.104:9876");
        // 3.启动producer
        producer.start();
        for (int i = 0; i < 10; i++) {
            // 4.创建消息对象，指定主题Topic、Tag和消息体
            Message message = new Message(
                    "TopicTest" /*Topic*/,
                    "tagA" /*Tag*/,
                    ("Hello RocketMQ, test sync: " + i).getBytes(RemotingHelper.DEFAULT_CHARSET) /*Message body*/
            );
            // 5.发送消息到一个Broker
            SendResult result = producer.send(message);
            // 发送状态
            SendStatus status = result.getSendStatus();
            // 消息ID
            String msgId = result.getMsgId();
            // 消息接收队列ID
            int queueId = result.getMessageQueue().getQueueId();
            log.debug("通过SendResult返回消息是否成功送达：{}，发送状态：{}，消息ID：{}，消息接收队列ID：{}", result, status, msgId, queueId);
        }
        // 6. 如果不再发送消息，关闭生产者producer
        producer.shutdown();
    }
}
