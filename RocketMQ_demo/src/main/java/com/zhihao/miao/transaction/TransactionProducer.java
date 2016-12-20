package com.zhihao.miao.transaction;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.LocalTransactionState;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.client.producer.TransactionCheckListener;
import com.alibaba.rocketmq.client.producer.TransactionMQProducer;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageExt;

/**
 * 1.�������ѵ�����˵һ��
 * 1)�����ڱ��������ύ֮ǰ����MQ����������һ��Prepared��Ϣ��
 * 2)�������������߼�
 * 3)�����������ɹ�������һ���ɹ���־��LocalTransactionState.COMMIT_MESSAGE����MQ����������ʱ�ͻ��˾�����MQ�������Ͽ��������Ϣ
 * 4)����������񲻳ɹ�����ôProducer�ͻᷢ��һ��ʧ�ܵı�־��LocalTransactionState.ROLLBACK_MESSAGE��MQ����������ʱ�����Ϣ���Ѷ˵�ǰ�ǿ�������
 * 5)��������ڱ���������ɻ�ʧ�ܵ�ʱ��Producer�ᷢ��һ����Ӧ�ı�־��MQ���������������׶ε��磬���û�������������ô��ʱMQ�ϵ������Ϣһֱ��Prepared��Ϣ��
 * RocketMQ�ᶨ��ɨ����Ϣ��Ⱥ�е�������Ϣ����ʱ������Prepared��Ϣ����������Ϣ������ȷ�ϣ�������Ǵ�����transactionCheckListenerʵ����Ҫ�������飬��Ȼ�ⲿ��ʵ�ֹ��ܱ������˸���
 * ��Ϊ���µ�����汾����ѿ�Դ�ģ��շѵ�����汾��֧�ֵģ�Ҳ��������������ϴ���������Ҫ�������飬
 * 
 * 
 * �ٷ�����demo����transactionCheckListener�ǻز�Ҫִ�еģ���TransactionExecuterImpl tranExecuter = new TransactionExecuterImpl();
 * ���ڷ�����Ϣ��ʱ������һ���߳�ִ�б��ص���صĲ�������Դ�汾��mq����û��ʵ������ز�Ĳ��֡�
 * 
 */
public class TransactionProducer {
    public static void main(String[] args) throws MQClientException, InterruptedException {
        
    	/**
    	 * δ������MQ����ز�ͻ���
    	 * �����TransactionCheckListenerImpl���Listenerʵ�ֲ�������������
    	 */
        TransactionCheckListener transactionCheckListener = new TransactionCheckListenerImpl();
        TransactionMQProducer producer = new TransactionMQProducer("transaction_group_name");
        producer.setNamesrvAddr("192.168.5.121:9876;192.168.5.122:9876");
        // ����ز���С������
        producer.setCheckThreadPoolMinSize(2);
        // ����ز���󲢷���
        producer.setCheckThreadPoolMaxSize(2);
        // ������
        producer.setCheckRequestHoldMax(2000);
        //����������ϴ�����
        producer.setTransactionCheckListener(transactionCheckListener);
        //�������ص�Producer����鱾�������֧�ɹ�����ʧ��
        /*
        producer.setTransactionCheckListener(new TransactionCheckListener() {
			@Override
			public LocalTransactionState checkLocalTransactionState(MessageExt msg) {
				System.out.println("state -- "+new String(msg.getBody()));
				//if���ݿ������ʵ�����仯�����ٴ��ύ״̬
				//else ���ݿ�û�з�˿���仯����ֱ�Ӻ��Ը����ݻع����ɡ�
				return LocalTransactionState.COMMIT_MESSAGE;
			}
		});
		*/
        producer.start();

        TransactionExecuterImpl tranExecuter = new TransactionExecuterImpl();
        for (int i = 1; i <= 2; i++) {
            try {
                Message msg =new Message("TopicTransaction", "Transaction"+i, "KEY" + i,
                            ("Hello RocketMQ" + i).getBytes());
                //�����������������һ����������Message���ڶ����������Ǳ�����������ࣨ�ص������ࣩ��������������߼���������ʵ�����н��еģ������������Ǹ��Ӳ��������ݵ��ڶ����ص������е�
                //��˼�����ڷ�����Ϣ��ͬʱ���ڶ��������ǻص�������ִ�б��ص�����������������������Ƕ����̡߳�
                SendResult sendResult = producer.sendMessageInTransaction(msg, tranExecuter, "hehe");
                System.out.println(sendResult);

                Thread.sleep(10);
            }
            catch (MQClientException e) {
                e.printStackTrace();
            }
        }

        for (int i = 0; i < 100000; i++) {
            Thread.sleep(1000);
        }

        producer.shutdown();

    }
}


