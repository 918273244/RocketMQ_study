package com.zhihao.miao.transaction;

import com.alibaba.rocketmq.client.producer.LocalTransactionExecuter;
import com.alibaba.rocketmq.client.producer.LocalTransactionState;
import com.alibaba.rocketmq.common.message.Message;

/**
 * �����ʵ��LocalTransactionExecuter��������������ģ�����ɹ�����COMMIT_MESSAGE������ʧ�ܷ���ROLLBACK_MESSAGE������һ������ͷ���UNKNOW��������Ƕ���ɨ������״̬����Ϣ
 * ����֤�������������ҷ��˶������������˵ڶ�������Tag����Transaction2����һ��
 */
public class TransactionExecuterImpl implements LocalTransactionExecuter {
    //private AtomicInteger transactionIndex = new AtomicInteger(1);


    @Override
    public LocalTransactionState executeLocalTransactionBranch(final Message msg, final Object arg) {
       //int value = transactionIndex.getAndIncrement();
    	System.out.println("msg="+new String(msg.getBody()));
    	//���arg���Ǹղŵĸ��Ӳ���
    	System.out.println("arg="+arg);
        String tag = msg.getTags();
        if (tag.equals("Transaction1")) {
        	System.out.println("���ﴦ�����߼�ʧ�ܣ�������Rollback");
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
