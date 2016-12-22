package com.zhihao.miao.retry;

import java.util.List;
import java.util.concurrent.TimeUnit;

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
 * 
 * 失败重试，消费者这边有二种情况，
 * 
 * 一种是消费没有返回ConsumeConcurrentlyStatus.RECONSUME_LATER或者ConsumeConcurrentlyStatus.CONSUME_SUCCESS给消费中间件，那么消息中间件就会一直不停的发消息给消费者，也就是time out重试机制
 * 还有一种就是exception重试机制，就会执行某一条消息的时候，如果某条消息失败，返回ConsumeConcurrentlyStatus.RECONSUME_LATER状态给中间件，那么就会间隔1s，2s，5s.....2h给消费者重新发送这条失败的消息，直到成功，当然列子2会在其发送二次的时候人工的解决这个问题。
 * 
 * MessageExt [queueId=0, storeSize=283, queueOffset=6, sysFlag=0, bornTimestamp=1466947320632, bornHost=/192.168.1.5:63412, 
 * storeTimestamp=1466947333592, storeHost=/192.168.1.121:10911, msgId=C0A8017900002A9F00000000000CAA9A, commitLogOffset=830106, 
 * bodyCRC=761548184, reconsumeTimes=1, preparedTransactionOffset=0, toString()=Message [topic=TopicTest, flag=0, properties={ORIGIN_MESSAGE_ID=C0A8017900002A9F0000000000086CA4, 
 * DELAY=3, REAL_TOPIC=%RETRY%quickstart_consumer, TAGS=TagA, WAIT=false, RETRY_TOPIC=TopicTest, MAX_OFFSET=10, MIN_OFFSET=0, REAL_QID=0}, body=15]]
 * 
 * 这个reconsumeTimes=1就是失败重新发送次数
 * 
 * 
 */
public class Comsumer {

    public static void main(String[] args) throws InterruptedException, MQClientException {
    	//quickstart_consumer也是ground Name也要全局唯一
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("topicRetry_consumer");
        consumer.setNamesrvAddr("192.168.5.121:9876;192.168.5.122:9876");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        //设置了从中间件拉取了10条消费数据，但是最后取出来还是一条一条从中间件取出来，这边是个误区，下面的List<MessageExt> msgs是1，那是不是这个参数设置就没有用了呢？显然不是，是因为我们RocketMQ是订阅发布模式，肯定要先启客户端，
    	//但是如果你先启动的是消费端，造成了数据的堆积，那么就可以多条多条的拉取
        consumer.setConsumeMessageBatchMaxSize(10);  
        
        consumer.subscribe("TopicRetry", "*");  //指定消息的过滤条件，这边没有指定，说明全局接收

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
            			//为了模拟出本来是Consumer1拿到消息，但是在返回ack确认机制的时候时间太长，并且我断掉服务，此时转由Comsumer进行消费。
            			//TimeUnit.SECONDS.sleep(20);
            			//打印出原始的id
            			String originMessageId = msg.getProperties().get(MessageConst.PROPERTY_ORIGIN_MESSAGE_ID);
            			System.out.println("originMessageId ==="+originMessageId);
            			//异常
            			//int a = 1/0;
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