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
 * 1.consumer.updateConsumeOffset(mq, pullResult.getNextBeginOffset());就是每隔多久帮我们记录一下拉取的位置，并且同步，这样我们就不要人为的记录位置到mysql或者redis等等
 * 2.就是这种拉取消费没有返回值，也就是失败之后没有重试机制，需要自己人为的做日志等等处理，保证消息的消费
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
                    //获取从哪里拉取
                    long offset = consumer.fetchConsumeOffset(mq, false);
                    if (offset < 0)
                        offset = 0;
                    
                    //第一个参数是mq参数，第二个是过滤的条件，第三个参数是拉取的位置，第四个参数是最多一次性拉取的个数
                    PullResult pullResult = consumer.pull(mq, "*", offset, 32);
                    //不停的拉取。
                    System.out.println(offset + "\t" + mq + "\t" + pullResult);
                    switch (pullResult.getPullStatus()) {
                    //有消息，拉取消息
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
                    case OFFSET_ILLEGAL:
                        break;
                    default:
                        break;
                    }

                    // 存储Offset，客户端每隔5s会定时刷新到Broker
                    consumer.updateConsumeOffset(mq, pullResult.getNextBeginOffset());

                    // 设置再过100ms后重新拉取
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
