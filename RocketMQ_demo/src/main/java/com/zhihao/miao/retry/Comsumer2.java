package com.zhihao.miao.retry;

import java.util.List;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageConst;
import com.alibaba.rocketmq.common.message.MessageExt;

/**
 * Consumer，订阅消息
 * MessageExt [queueId=0, storeSize=283, queueOffset=6, sysFlag=0, bornTimestamp=1466947320632, bornHost=/192.168.1.5:63412, 
 * storeTimestamp=1466947333592, storeHost=/192.168.1.121:10911, msgId=C0A8017900002A9F00000000000CAA9A, commitLogOffset=830106, 
 * bodyCRC=761548184, reconsumeTimes=1, preparedTransactionOffset=0, toString()=Message [topic=TopicTest, flag=0, properties={ORIGIN_MESSAGE_ID=C0A8017900002A9F0000000000086CA4, 
 * DELAY=3, REAL_TOPIC=%RETRY%quickstart_consumer, TAGS=TagA, WAIT=false, RETRY_TOPIC=TopicTest, MAX_OFFSET=10, MIN_OFFSET=0, REAL_QID=0}, body=15]]
 * 
 * 这个reconsumeTimes=1就是失败重新发送次数
 * 
 * MessageExt [queueId=0, storeSize=283, queueOffset=21, sysFlag=0, bornTimestamp=1466947334272, bornHost=/192.168.1.5:63423, storeTimestamp=1466947371321, storeHost=/192.168.1.121:10911, 
 * msgId=C0A8017900002A9F00000000000CC843, commitLogOffset=837699, bodyCRC=761548184, reconsumeTimes=2, preparedTransactionOffset=0, toString()=Message [topic=TopicTest, flag=0, 
 * properties={ORIGIN_MESSAGE_ID=C0A8017900002A9F00000000000A8E37, DELAY=4, REAL_TOPIC=%RETRY%quickstart_consumer, 
 * TAGS=TagA, WAIT=false, RETRY_TOPIC=TopicTest, MAX_OFFSET=24, MIN_OFFSET=0, REAL_QID=0}, body=15]]
 * 
 * 
 * 其实说白了exception重试策略就是返回ConsumeConcurrentlyStatus的状态值，如果是达到自己设定的重试次数，那么就不要再去发送了
 */
public class Comsumer2 {

    public static void main(String[] args) throws InterruptedException, MQClientException {
    	//quickstart_consumer也是ground Name也要全局唯一
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("topicRetry_consumer");
        consumer.setNamesrvAddr("192.168.5.121:9876;192.168.5.122:9876");
        /**
         * 设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费<br>
         * 如果非第一次启动，那么按照上次消费的位置继续消费
         */
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        //设置了从中间件拉取了10条消费数据，但是最后取出来还是一条一条从中间件取出来，这边是个误区，下面的List<MessageExt> msgs是1，那是不是这个参数设置就没有用了呢？显然不是，是因为我们RocketMQ是订阅发布模式，肯定要先启客户端，
    	//但是如果你先启动的是消费端，造成了数据的堆积，那么就可以多条多条的拉取
        consumer.setConsumeMessageBatchMaxSize(10);  
        
        consumer.subscribe("TopicRetry", "*");  //指定消息的过滤条件，这边没有指定，说明全局接收

        consumer.registerMessageListener(new MessageListenerConcurrently() {

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,ConsumeConcurrentlyContext context) {
            	//这边还是按正常的先启动消费端再去启动客户端，那么每次接收一条消息
            	MessageExt message = (MessageExt)msgs.get(0);
            	try{
            		String topic = message.getTopic();
        			String msgBody = new String(message.getBody(),"utf-8");
        			String tags = message.getTags();
        			System.out.println("收到消息：topic"+topic+",tags:"+tags+",msg:"+msgBody);
					System.out.println(message);
					System.out.println(msgBody);
					String originMessageId = message.getProperties().get(MessageConst.PROPERTY_ORIGIN_MESSAGE_ID);
        			System.out.println("originMessageId ==="+originMessageId);
					// 异常
					//int a = 1 / 0;
            	}catch(Exception e){
            		if(message.getReconsumeTimes() == 2){
            			//记录到数据库日志，不要重新发了
            			e.printStackTrace();
            			System.out.println("记录到日志了，不会再去发送了");
            			return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            		}else{
            			return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            		}
            	}
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;   //指的是消费成功之后给mq发送一个消息
            }
        });

        consumer.start();

        System.out.println("Consumer Started.");
    }
}
