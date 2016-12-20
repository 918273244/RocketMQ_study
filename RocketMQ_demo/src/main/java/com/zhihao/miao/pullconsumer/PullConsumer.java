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
 * 1.PullConsumer,ʹ��Pull������PullConsumer
 * 2.��ȡ����һ��Ҫ��¼λ�ã�����Ҫ��ÿ����ȡ��λ�ü�¼�����ݿ��У���Ȼ�Ժ�����ѻ��������ѣ�������ݵ��ظ�����
 */
public class PullConsumer {
	//���������HashMap�������ȡ���ݵ�λ�ã������´λ�ȡ���ѵ�λ�ã�������ظ�����
    private static final Map<MessageQueue, Long> offseTable = new HashMap<MessageQueue, Long>();


    public static void main(String[] args) throws MQClientException {
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("pull_consumer_group");
        consumer.setNamesrvAddr("192.168.5.121:9876;192.168.5.122:9876");
        consumer.start();

        Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues("TopicPull");
        //��߾���һ�������µĶ�����н�����ȡ��Ȼ���¼ÿ��������ȡ���ѵ�λ��
        for (MessageQueue mq : mqs) {
            System.out.println("Consume from the queue: " + mq);
            SINGLE_MQ: while (true) {
                try {
                	//pullBlockIfNotFound��һ��������mq���ڶ��������ǹ��˱��ʽ����������������ȡ��λ�ã����ĸ�������һ���������ȡ���ٸ���Ϣ
                    PullResult pullResult =consumer.pullBlockIfNotFound(mq, null, getMessageQueueOffset(mq), 32);
                    System.out.println(pullResult);
                    System.out.println(pullResult.getPullStatus());
                    //pullResult.getNextBeginOffset()��һ����ȡ��Ϣ��λ��
                    putMessageQueueOffset(mq, pullResult.getNextBeginOffset());
                    switch (pullResult.getPullStatus()) {
                    //��ȡ���µ���Ϣ
                    case FOUND:
                    	List<MessageExt> list = pullResult.getMsgFoundList();
                    	for(MessageExt msg:list){
                    		System.out.println(new String(msg.getBody()));
                    	}
                        break;
                    //û��ƥ�����Ϣ
                    case NO_MATCHED_MSG:
                        break;
                    //û���µ���Ϣ
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

