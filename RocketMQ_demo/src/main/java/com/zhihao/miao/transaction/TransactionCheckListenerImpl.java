package com.zhihao.miao.transaction;

import com.alibaba.rocketmq.client.producer.LocalTransactionState;
import com.alibaba.rocketmq.client.producer.TransactionCheckListener;
import com.alibaba.rocketmq.common.message.MessageExt;

/**
 * 未决事务，服务器回查客户端
 * 这边的代码是这样的，但是却没有实现，平安买的收费版的就是实现的
 * 
 * 如果返回的是LocalTransactionState.ROLLBACK_MESSAGE 说明本地执行消息失败，删除消息
 * 如果返回LocalTransactionState.COMMIT_MESSAGE则将Broker之前发送的Prepared消息进行可视化
 * 如果返回LocalTransactionState.UNKNOW 这种状态就是本地事务成功了发消息到mq服务上，进行ack确认机制，但是没有收到ack确认，返回unknow，此时这个就是不断轮询的查询重发机制。
 */
public class TransactionCheckListenerImpl implements TransactionCheckListener {
	// private AtomicInteger transactionIndex = new AtomicInteger(0);

	@Override
	public LocalTransactionState checkLocalTransactionState(MessageExt msg) {
		// System.out.println("server checking TrMsg " + msg.toString());

		System.out.println("msg=" + new String(msg.getBody()));
		String tag = msg.getTags();
		if (tag.equals("Transaction1")) {
			System.out.println("这里处理本地逻辑失败，进行了Rollback");
			return LocalTransactionState.ROLLBACK_MESSAGE;
		} else if (tag.equals("Transaction2")) {
			return LocalTransactionState.COMMIT_MESSAGE;
		} else {
			return LocalTransactionState.UNKNOW;
		}
	}
}
