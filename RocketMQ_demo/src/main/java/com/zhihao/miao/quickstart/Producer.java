package com.zhihao.miao.quickstart;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;

public class Producer {
	 public static void main(String[] args) throws MQClientException, InterruptedException {
		    //quickstart_producer这个是ground Name全局唯一，就是一个web项目要唯一
	        DefaultMQProducer producer = new DefaultMQProducer("quickstart_producer");
	        producer.setNamesrvAddr("192.168.5.121:9876;192.168.5.122:9876");
	        //这个校验会在send方法中校验
	        //producer.setMaxMessageSize(maxMessageSize);
	        producer.start();
	        /*
	        producer.setMaxMessageSize(1024);
	        byte[] buf = new byte[1023];
	        for(int i = 0;i<buf.length;i++){
	        	buf[i] = (byte)i;
	        }
	        producer.start();
	        Message msg = new Message("TopicTest","TagA",buf);
	        SendResult sendResult = null;
			try {
				sendResult = producer.send(msg);
			} catch (RemotingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (MQBrokerException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	        System.out.println(sendResult);
	        */
	        
	        for (int i = 0; i < 10; i++) {
	            try {
	                Message msg = new Message("TopicTest",// topic
	                    "TagA",//tag,过滤条件
	                    ("Hello RocketMQ " + i).getBytes()// body
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
