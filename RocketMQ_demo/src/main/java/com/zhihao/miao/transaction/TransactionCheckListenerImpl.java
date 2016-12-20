package com.zhihao.miao.transaction;

import com.alibaba.rocketmq.client.producer.LocalTransactionState;
import com.alibaba.rocketmq.client.producer.TransactionCheckListener;
import com.alibaba.rocketmq.common.message.MessageExt;

/**
 * δ�����񣬷������ز�ͻ���
 * ��ߵĴ����������ģ�����ȴû��ʵ�֣�ƽ������շѰ�ľ���ʵ�ֵ�
 * 
 * ������ص���LocalTransactionState.ROLLBACK_MESSAGE ˵������ִ����Ϣʧ�ܣ�ɾ����Ϣ
 * �������LocalTransactionState.COMMIT_MESSAGE��Broker֮ǰ���͵�Prepared��Ϣ���п��ӻ�
 * �������LocalTransactionState.UNKNOW ����״̬���Ǳ�������ɹ��˷���Ϣ��mq�����ϣ�����ackȷ�ϻ��ƣ�����û���յ�ackȷ�ϣ�����unknow����ʱ������ǲ�����ѯ�Ĳ�ѯ�ط����ơ�
 */
public class TransactionCheckListenerImpl implements TransactionCheckListener {
	// private AtomicInteger transactionIndex = new AtomicInteger(0);

	@Override
	public LocalTransactionState checkLocalTransactionState(MessageExt msg) {
		// System.out.println("server checking TrMsg " + msg.toString());

		System.out.println("msg=" + new String(msg.getBody()));
		String tag = msg.getTags();
		if (tag.equals("Transaction1")) {
			System.out.println("���ﴦ�����߼�ʧ�ܣ�������Rollback");
			return LocalTransactionState.ROLLBACK_MESSAGE;
		} else if (tag.equals("Transaction2")) {
			return LocalTransactionState.COMMIT_MESSAGE;
		} else {
			return LocalTransactionState.UNKNOW;
		}
	}
}
