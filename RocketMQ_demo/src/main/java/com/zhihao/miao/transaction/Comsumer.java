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
    	//quickstart_consumerҲ��ground NameҲҪȫ��Ψһ
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("transaction_consumer");
        consumer.setNamesrvAddr("192.168.5.121:9876;192.168.5.122:9876");
        /**
         * ����Consumer��һ�������ǴӶ���ͷ����ʼ���ѻ��Ƕ���β����ʼ����<br>
         * ����ǵ�һ����������ô�����ϴ����ѵ�λ�ü�������
         */
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        //�����˴��м����ȡ��10���������ݣ��������ȡ��������һ��һ�����м��ȡ����������Ǹ������������List<MessageExt> msgs��1�����ǲ�������������þ�û�������أ���Ȼ���ǣ�����Ϊ����RocketMQ�Ƕ��ķ���ģʽ���϶�Ҫ�����ͻ��ˣ�
    	//����������������������Ѷˣ���������ݵĶѻ�����ô�Ϳ��Զ�����������ȡ
        consumer.setConsumeMessageBatchMaxSize(10);  
        
        consumer.subscribe("TopicTransaction", "*");  //ָ����Ϣ�Ĺ������������û��ָ����˵��ȫ�ֽ���

        consumer.registerMessageListener(new MessageListenerConcurrently() {

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,ConsumeConcurrentlyContext context) {
            	System.out.println(msgs.size());
            	try{
            		for(MessageExt msg:msgs){
            			String topic = msg.getTopic();
            			String msgBody = new String(msg.getBody(),"utf-8");
            			String tags = msg.getTags();
            			System.out.println("�յ���Ϣ��topic"+topic+",tags:"+tags+",msg:"+msgBody);
            		}
            	}catch(Exception e){
            		e.printStackTrace();
            		//1s 2s 5s ......2h����߾��������ļ������õ���Ϣʧ���ط�ʱ����
            		return ConsumeConcurrentlyStatus.RECONSUME_LATER;   //ʧ���Ժ����·���
            	}
                //System.out.println(Thread.currentThread().getName() + " Receive New Messages: " + msgs);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;   //ָ�������ѳɹ�֮���mq����һ����Ϣ
            }
        });

        consumer.start();

        System.out.println("Consumer Started.");
    }
}
