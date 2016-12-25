package com.zhihao.miao.filter;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;

public class Producer {
    public static void main(String[] args) throws MQClientException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("ProducerGroupName");
        producer.setNamesrvAddr("192.168.5.121:9876;192.168.5.122:9876");
        producer.start();

        try {
            for (int i = 0; i < 50; i++) {
                Message msg = new Message("TopicFilter2",// topic
                    "TagA",// tag
                    "OrderID001",// key
                    ("Hello RocketMQ"+i).getBytes());// body
                
                //和ActiveMQ一样也是最好对属性过滤
                msg.putUserProperty("SequenceId", String.valueOf(i));

                SendResult sendResult = producer.send(msg);
                System.out.println(sendResult);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        producer.shutdown();
    }
}
