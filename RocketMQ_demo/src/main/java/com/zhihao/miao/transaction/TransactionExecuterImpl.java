package com.zhihao.miao.transaction;

import com.alibaba.rocketmq.client.producer.LocalTransactionExecuter;
import com.alibaba.rocketmq.client.producer.LocalTransactionState;
import com.alibaba.rocketmq.common.message.Message;

/**
 * 这个类实现LocalTransactionExecuter，做本地事务处理的，事务成功返回COMMIT_MESSAGE，事务失败返回ROLLBACK_MESSAGE，另外一个情况就返回UNKNOW，这个就是定期扫描事务状态的消息
 * 事务证明本地事务处理我发了二条，被消费了第二条就是Tag等于Transaction2的这一条
 */
public class TransactionExecuterImpl implements LocalTransactionExecuter {
    //private AtomicInteger transactionIndex = new AtomicInteger(1);


    @Override
    public LocalTransactionState executeLocalTransactionBranch(final Message msg, final Object arg) {
       //int value = transactionIndex.getAndIncrement();
    	System.out.println("msg="+new String(msg.getBody()));
    	//这个arg就是刚才的附加参数
    	System.out.println("arg="+arg);
        String tag = msg.getTags();
        if (tag.equals("Transaction1")) {
        	System.out.println("这里处理本地逻辑失败，进行了Rollback");
        	return LocalTransactionState.ROLLBACK_MESSAGE;
        }
        else if (tag.equals("Transaction2")) {
        	return LocalTransactionState.COMMIT_MESSAGE;
        }
        else{
        	return LocalTransactionState.UNKNOW;
        }

    }
}
