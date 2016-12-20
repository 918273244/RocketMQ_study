package com.zhihao.miao.transaction;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.LocalTransactionState;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.client.producer.TransactionCheckListener;
import com.alibaba.rocketmq.client.producer.TransactionMQProducer;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageExt;

/**
 * 1.事务消费的流程说一下
 * 1)就是在本地事务提交之前，向MQ服务器发送一个Prepared消息，
 * 2)做本地事务处理逻辑
 * 3)如果本地事务成功，发送一个成功标志（LocalTransactionState.COMMIT_MESSAGE）给MQ服务器，此时客户端就能在MQ服务器上看到这个消息
 * 4)如果本地事务不成功，那么Producer就会发送一个失败的标志给LocalTransactionState.ROLLBACK_MESSAGE给MQ服务器，此时这个消息消费端当前是看不到的
 * 5)就是如果在本地事务完成或失败的时候，Producer会发送一个相应的标志给MQ服务器，如果这个阶段掉电，造成没有这个反馈，那么此时MQ上的这个消息一直是Prepared消息，
 * RocketMQ会定期扫描消息集群中的事务消息，这时候发现了Prepared消息，它会向消息发送者确认，这个就是此列中transactionCheckListener实现类要做的事情，当然这部分实现功能被阿里阉割了
 * 因为最新的这个版本是免费开源的，收费的这个版本是支持的，也就是设置事务决断处理类中所要做的事情，
 * 
 * 
 * 官方给的demo就是transactionCheckListener是回查要执行的，而TransactionExecuterImpl tranExecuter = new TransactionExecuterImpl();
 * 是在发送消息的时候另起一个线程执行本地的相关的操作，开源版本的mq就是没有实现这个回查的部分。
 * 
 */
public class TransactionProducer {
    public static void main(String[] args) throws MQClientException, InterruptedException {
        
    	/**
    	 * 未决事务，MQ服务回查客户端
    	 * 会根据TransactionCheckListenerImpl这个Listener实现策略来绝断事务
    	 */
        TransactionCheckListener transactionCheckListener = new TransactionCheckListenerImpl();
        TransactionMQProducer producer = new TransactionMQProducer("transaction_group_name");
        producer.setNamesrvAddr("192.168.5.121:9876;192.168.5.122:9876");
        // 事务回查最小并发数
        producer.setCheckThreadPoolMinSize(2);
        // 事务回查最大并发数
        producer.setCheckThreadPoolMaxSize(2);
        // 队列数
        producer.setCheckRequestHoldMax(2000);
        //设置事务决断处理类
        producer.setTransactionCheckListener(transactionCheckListener);
        //服务器回调Producer，检查本地事务分支成功还是失败
        /*
        producer.setTransactionCheckListener(new TransactionCheckListener() {
			@Override
			public LocalTransactionState checkLocalTransactionState(MessageExt msg) {
				System.out.println("state -- "+new String(msg.getBody()));
				//if数据库入库真实发生变化，则再次提交状态
				//else 数据库没有发丝昂变化，则直接忽略该数据回滚即可。
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
                //这边有三个参数，第一个参数就是Message，第二个参数就是本地事务处理的类（回调函数类），本地事务处理的逻辑就是在其实现类中进行的，第三个参数是附加参数，传递到第二个回调函数中的
                //意思就是在发送消息的同时，第二个参数是回调函数，执行本地的事务操作操作，二个操作是二个线程。
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


