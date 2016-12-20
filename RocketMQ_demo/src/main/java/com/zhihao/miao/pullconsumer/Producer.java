package com.zhihao.miao.pullconsumer;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;

public class Producer {
    public static void main(String[] args) throws MQClientException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("pull_producer_group");
        producer.setNamesrvAddr("192.168.5.121:9876;192.168.5.122:9876");
        producer.setRetryTimesWhenSendFailed(10);  //��ʾ������Ͷ����ѷ���ʧ��֮������10�η���
        producer.start();

        for (int i = 0; i < 40; i++) {
            try {
                Message msg = new Message("TopicPull",// topic
                    "TagA",// tag,��������
                    ("Hello RocketMQ" + i).getBytes()// body
                        );
                SendResult sendResult = producer.send(msg);
                System.out.println(sendResult);
            }
            catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(1000);
            }
        }

        producer.shutdown();
    }
}

