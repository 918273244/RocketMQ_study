/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.zhihao.miao.ordermessage;

import java.util.List;
import java.util.Random;

import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.MQProducer;
import com.alibaba.rocketmq.client.producer.MessageQueueSelector;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.remoting.exception.RemotingException;


/**
 * Producer，发送顺序消息
  * Producer，发送顺序消息
 * 一个订单产生了 3 条消息，分别是订单创建、订单付款、订单完成。消费时，要按照这个顺序消费才有意义。但同时订单之间又是可以并行消费的。
 * 
 * 所有消息都是放在一个Topic下，只是不同的需要顺序消费的消息放在同一个队列，并且同一个队列中的消息还要推送到同一个Broker（也就是节点的意思），并且同一个队列（需要顺序消费的消息也需要推送到
 * 同一个消费端）这样就保证了消息的顺序消费 
 */
public class Producer {
	
    public static void main(String[] args) {
        try {
        	final DefaultMQProducer producer = new DefaultMQProducer("quickstart_producer");
            producer.setNamesrvAddr("192.168.5.121:9876;192.168.5.122:9876");
            producer.start();
            Thread t1 = new Thread(new Runnable() {
    			@Override
    			public void run() {
    				for (int i = 0; i < 10; i++) {
    	                Message msg =
    	                        new Message("TopicTestOrder", "tag"+i, "KEY" + i,
    	                            ("Hello RocketMQ0 " + i).getBytes());
    	                //发送数据，如果是顺序消费，必须实现自己的MessageQueueSelector，保证需要顺序消费的消息进入同一队列
    	                SendResult sendResult = null;
						try {
							sendResult = producer.send(msg, new MessageQueueSelector() {
							    @Override
							    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
							        Integer id = (Integer) arg;
							        int index = id % mqs.size();
							        return mqs.get(index);
							    }
							}, 0);//orderId是队列的下标
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}  

    	                System.out.println(sendResult);
    	            }
    			}
    		});
            
            Thread t2 = new Thread(new Runnable() {
    			@Override 
    			public void run() {
    				for (int i = 0; i < 10; i++) {
    	                Message msg =
    	                        new Message("TopicTestOrder", "tag"+i, "KEY" + i,
    	                            ("Hello RocketMQ1 " + i).getBytes());
    	                //发送数据，如果是顺序消费，必须实现自己的MessageQueueSelector，保证需要顺序消费的消息进入同一队列
    	                SendResult sendResult = null;
						try {
							sendResult = producer.send(msg, new MessageQueueSelector() {
							    @Override
							    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
							        Integer id = (Integer) arg;
							        int index = id % mqs.size();
							        return mqs.get(index);
							    }
							}, 1);//orderId是队列的下标
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}  

    	                System.out.println(sendResult);
    	            }
    			}
    		});
            
            Thread t3 = new Thread(new Runnable() {
				@Override
				public void run() {
					for (int i = 0; i < 10; i++) {
		                Message msg =
		                        new Message("TopicTestOrder", "tag"+i, "KEY" + i,
		                            ("Hello RocketMQ2 " + i).getBytes());
		                //发送数据，如果是顺序消费，必须实现自己的MessageQueueSelector，保证需要顺序消费的消息进入同一队列
		                SendResult sendResult = null;
						try {
							sendResult = producer.send(msg, new MessageQueueSelector() {
							    @Override
							    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
							        Integer id = (Integer) arg;
							        int index = id % mqs.size();
							        return mqs.get(index);
							    }
							}, 2); //orderId是队列的下标
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}  

		                System.out.println(sendResult);
		            }
					
				}
			});
            
            
            t1.start();
            t2.start();
            t3.start();
            
            
            //这边睡眠20s，因为主函数线程和线程执行是不同的线程，如果不阻塞的话，会造成主函数关闭procuder而多线程没有执行完，就会抛出异常、
            Thread.sleep(10*1000L);
            
            
           
            producer.shutdown();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
