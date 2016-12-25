package com.zhihao.miao.filter;

import java.io.UnsupportedEncodingException;
import java.util.List;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.message.MessageExt;

/**
 * 其实所有生产端的消息都发到broker上了，只是多了一层filter服务专门过滤我们业务要求的，比如这个列子中的MessageFilterImpl就是过滤掉一些自己的业务需求的一些消息
 * @author Administrator
 *
 */
public class Consumer {

    public static void main(String[] args) throws InterruptedException, MQClientException {
    	String group_name ="filter_consumer";
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group_name);
        consumer.setNamesrvAddr("192.168.5.121:9876;192.168.5.122:9876");
        //MessageFilterImpl类中如果有中文，包括中文注释，这个MixAll.file2String工具类就不生效了
        //只能是绝对路径的过滤代码类
        String filterCode = MixAll.file2String("E:\\jiagou2\\RocketMQ\\RocketMQ_study\\RocketMQ_demo\\src\\main\\java\\com\\zhihao\\miao\\filter\\MessageFilterImpl.java");
        //String filterCode = MixAll.file2String("MessageFilterImpl.java");
        System.out.println(filterCode);
        /**
         * 使用Java代码，在服务器做消息过滤
         */
        //consumer.subscribe("TopicFilter7", MessageFilterImpl.class.getCanonicalName());
        consumer.subscribe("TopicFilter2", "com.zhihao.miao.filter.MessageFilterImpl",filterCode);

        consumer.registerMessageListener(new MessageListenerConcurrently() {

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                    ConsumeConcurrentlyContext context) {
//                System.out.println(Thread.currentThread().getName() + " Receive New Messages: " + msgs);
            	MessageExt me = msgs.get(0);
                try {
					System.out.println("收到信息："+new String(me.getBody(),"utf-8"));
				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				}
            	return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        /**
         * Consumer对象在使用之前必须要调用start初始化，初始化一次即可<br>
         */
        consumer.start();

        System.out.println("Consumer Started.");
    }
}
