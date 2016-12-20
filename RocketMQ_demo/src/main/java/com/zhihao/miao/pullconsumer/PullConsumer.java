package com.zhihao.miao.pullconsumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;

/**
 * 1.PullConsumer,使用Pull消费者PullConsumer
 * 2.拉取消费一定要记录位置，并且要把每次拉取的位置记录到数据库中，不然以后的消费会重新消费，造成数据的重复消费
 */
public class PullConsumer {
	//这边我是用HashMap来存放拉取数据的位置，便于下次获取消费的位置，以造成重复消费
    private static final Map<MessageQueue, Long> offseTable = new HashMap<MessageQueue, Long>();


    public static void main(String[] args) throws MQClientException {
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("pull_consumer_group");
        consumer.setNamesrvAddr("192.168.5.121:9876;192.168.5.122:9876");
        consumer.start();

        Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues("TopicPull");
        //这边就是一个主题下的多个队列进行拉取，然后记录每个队列拉取消费的位置
        for (MessageQueue mq : mqs) {
            System.out.println("Consume from the queue: " + mq);
            SINGLE_MQ: while (true) {
                try {
                	//pullBlockIfNotFound第一个参数是mq，第二个参数是过滤表达式，第三个参数是拉取的位置，第四个参数是一次性最多拉取多少个消息
                    PullResult pullResult =consumer.pullBlockIfNotFound(mq, null, getMessageQueueOffset(mq), 32);
                    System.out.println(pullResult);
                    System.out.println(pullResult.getPullStatus());
                    //pullResult.getNextBeginOffset()下一次拉取消息的位置
                    putMessageQueueOffset(mq, pullResult.getNextBeginOffset());
                    switch (pullResult.getPullStatus()) {
                    //拉取到新的消息
                    case FOUND:
                    	List<MessageExt> list = pullResult.getMsgFoundList();
                    	for(MessageExt msg:list){
                    		System.out.println(new String(msg.getBody()));
                    	}
                        break;
                    //没有匹配的消息
                    case NO_MATCHED_MSG:
                        break;
                    //没有新的消息
                    case NO_NEW_MSG:
                        break SINGLE_MQ;
                    case OFFSET_ILLEGAL:
                        break;
                    default:
                        break;
                    }
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        consumer.shutdown();
    }


    private static void putMessageQueueOffset(MessageQueue mq, long offset) {
        offseTable.put(mq, offset);
    }


    private static long getMessageQueueOffset(MessageQueue mq) {
        Long offset = offseTable.get(mq);
        if (offset != null)
            return offset;

        return 0;
    }

}

