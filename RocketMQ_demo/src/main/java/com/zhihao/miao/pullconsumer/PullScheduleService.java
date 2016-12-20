package com.zhihao.miao.pullconsumer;

import java.util.List;

import com.alibaba.rocketmq.client.consumer.MQPullConsumer;
import com.alibaba.rocketmq.client.consumer.MQPullConsumerScheduleService;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.consumer.PullTaskCallback;
import com.alibaba.rocketmq.client.consumer.PullTaskContext;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;

/**
 * 1.consumer.updateConsumeOffset(mq, pullResult.getNextBeginOffset());����ÿ����ð����Ǽ�¼һ����ȡ��λ�ã�����ͬ�����������ǾͲ�Ҫ��Ϊ�ļ�¼λ�õ�mysql����redis�ȵ�
 * 2.����������ȡ����û�з���ֵ��Ҳ����ʧ��֮��û�����Ի��ƣ���Ҫ�Լ���Ϊ������־�ȵȴ�����֤��Ϣ������
 *
 */
public class PullScheduleService {

    public static void main(String[] args) throws MQClientException {
    	String group_name ="schedule_consumer";
    	
        final MQPullConsumerScheduleService scheduleService = new MQPullConsumerScheduleService(group_name);
        scheduleService.getDefaultMQPullConsumer().setNamesrvAddr("192.168.5.121:9876;192.168.5.122:9876");

        scheduleService.setMessageModel(MessageModel.CLUSTERING);
        scheduleService.registerPullTaskCallback("TopicPull", new PullTaskCallback() {

            @Override
            public void doPullTask(MessageQueue mq, PullTaskContext context) {
                MQPullConsumer consumer = context.getPullConsumer();
                try {
                    //��ȡ��������ȡ
                    long offset = consumer.fetchConsumeOffset(mq, false);
                    if (offset < 0)
                        offset = 0;
                    
                    //��һ��������mq�������ڶ����ǹ��˵���������������������ȡ��λ�ã����ĸ����������һ������ȡ�ĸ���
                    PullResult pullResult = consumer.pull(mq, "*", offset, 32);
                    //��ͣ����ȡ��
                    System.out.println(offset + "\t" + mq + "\t" + pullResult);
                    switch (pullResult.getPullStatus()) {
                    //����Ϣ����ȡ��Ϣ
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
                    case OFFSET_ILLEGAL:
                        break;
                    default:
                        break;
                    }

                    // �洢Offset���ͻ���ÿ��5s�ᶨʱˢ�µ�Broker
                    consumer.updateConsumeOffset(mq, pullResult.getNextBeginOffset());

                    // �����ٹ�100ms��������ȡ
                    context.setPullNextDelayTimeMillis(100);
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        scheduleService.start();
    }
}
