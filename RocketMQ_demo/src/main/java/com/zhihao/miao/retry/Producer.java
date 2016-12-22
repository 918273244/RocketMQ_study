package com.zhihao.miao.retry;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;

public class Producer {
	 public static void main(String[] args) throws MQClientException, InterruptedException {
		    //quickstart_producer这个是ground Name全局唯一，就是一个web项目要唯一
	        DefaultMQProducer producer = new DefaultMQProducer("topicRetry_producer");
	        producer.setNamesrvAddr("192.168.5.121:9876;192.168.5.122:9876");
	        producer.start();
	        
	        for (int i = 0; i < 1; i++) {
	            try {
	                Message msg = new Message("TopicRetry",// topic
	                    "TagA",//tag,过滤条件
	                    ("Hello RocketMQ " + 3).getBytes()// body
	                        );
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
