package com.zhihao.miao.ordermessage;

import java.util.List;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;


/**
 * 多个消费端指定了同一个group name，并且订阅的是同一个主题，所以实现了负载均衡，但是每个队列中的顺序的消息都会在一个客户端执行，因为这样保持了顺序消费
 * 这边的消费端的Listener实现的接口为MessageListenerOrderly而不是之前的MessageListenerConcurrently，最后返回的状态和实现的方法都不一样
 * 这个方法的意思就是表示一个线程只能访问一个队列里的消息，这个线程不能访问别的队列的消息，保证一个队列在一个线程中消费
 */
public class Consumer {

	public Consumer() {
		try {
			String group_name ="message_consumer_ordermessage";
			DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group_name);
			consumer.setNamesrvAddr("192.168.5.121:9876;192.168.5.122:9876");
			consumer.subscribe("TopicTestOrder", "*");
			consumer.setMessageModel(MessageModel.CLUSTERING);
			  /**
	         * 设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费<br>
	         * 如果非第一次启动，那么按照上次消费的位置继续消费
	         */
	        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
	        //订阅的主题以及过滤的标签内容
	        /**消费线程池最小数量：默认是10*/
	        consumer.setConsumeThreadMin(10);
	        /**消费线程池最大数量：默认是20*/
	        consumer.setConsumeThreadMax(20);
			consumer.registerMessageListener(new Listener());
			
			consumer.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	class Listener implements MessageListenerOrderly{
		@Override
		public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs,ConsumeOrderlyContext context) {
			try{
        		for(MessageExt msg:msgs){
        			String topic = msg.getTopic();
        			String msgBody = new String(msg.getBody(),"utf-8");
        			String tags = msg.getTags();
        			System.out.println("收到消息：topic "+topic+" ,tags:"+tags+" ,msg:"+msgBody);
        		}
        	}catch(Exception e){
        		e.printStackTrace();
        		return ConsumeOrderlyStatus.ROLLBACK; 
        	}
            return ConsumeOrderlyStatus.SUCCESS;   
		}
		
	}
	
	public static void main(String[] args) {
		Consumer c1 = new Consumer();
		System.out.println("c1 start.....");
	}

}
