package com.zhihao.miao.transaction;

import java.util.List;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;

public class Comsumer {

    public static void main(String[] args) throws InterruptedException, MQClientException {
    	//quickstart_consumer也是ground Name也要全局唯一
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("transaction_consumer");
        consumer.setNamesrvAddr("192.168.5.121:9876;192.168.5.122:9876");
        /**
         * 设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费<br>
         * 如果非第一次启动，那么按照上次消费的位置继续消费
         */
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        //设置了从中间件拉取了10条消费数据，但是最后取出来还是一条一条从中间件取出来，这边是个误区，下面的List<MessageExt> msgs是1，那是不是这个参数设置就没有用了呢？显然不是，是因为我们RocketMQ是订阅发布模式，肯定要先启客户端，
    	//但是如果你先启动的是消费端，造成了数据的堆积，那么就可以多条多条的拉取
        consumer.setConsumeMessageBatchMaxSize(10);  
        
        consumer.subscribe("TopicTransaction", "*");  //指定消息的过滤条件，这边没有指定，说明全局接收

        consumer.registerMessageListener(new MessageListenerConcurrently() {

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,ConsumeConcurrentlyContext context) {
            	System.out.println(msgs.size());
            	try{
            		for(MessageExt msg:msgs){
            			String topic = msg.getTopic();
            			String msgBody = new String(msg.getBody(),"utf-8");
            			String tags = msg.getTags();
            			System.out.println("收到消息：topic"+topic+",tags:"+tags+",msg:"+msgBody);
            		}
            	}catch(Exception e){
            		e.printStackTrace();
            		//1s 2s 5s ......2h，这边就是配置文件中配置的消息失败重发时间间隔
            		return ConsumeConcurrentlyStatus.RECONSUME_LATER;   //失败稍后重新发送
            	}
                //System.out.println(Thread.currentThread().getName() + " Receive New Messages: " + msgs);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;   //指的是消费成功之后给mq发送一个消息
            }
        });

        consumer.start();

        System.out.println("Consumer Started.");
    }
}
