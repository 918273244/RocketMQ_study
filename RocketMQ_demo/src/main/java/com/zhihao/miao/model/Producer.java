package com.zhihao.miao.model;

import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;

/**
 * 集群消费和广播消费，集群消费就是多个消费者负载均衡的进行消费，而广播消费是每个消费端都订阅消费了主题中的所有消息。
 * 
 */
public class Producer {
    public static void main(String[] args) throws Exception {
        	DefaultMQProducer producer = new DefaultMQProducer("message_consumer_model");
        	producer.setNamesrvAddr("192.168.5.121:9876;192.168.5.122:9876");

            producer.start();
            for (int i = 0; i < 16; i++) {
	            try {
	                Message msg = new Message("TopicTestModel",
	                    "TagA",
	                    ("Hello RocketMQ" + i).getBytes());
	                SendResult sendResult = producer.send(msg);
	                System.out.println(sendResult);
	            }
	            catch (Exception e) {
	                e.printStackTrace();
	                Thread.sleep(1000);
	            }
	        }

	        producer.shutdown();

            

    }       
}
