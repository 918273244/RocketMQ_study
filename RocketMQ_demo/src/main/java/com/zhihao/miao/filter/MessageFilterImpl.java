package com.zhihao.miao.filter;

import com.alibaba.rocketmq.common.filter.MessageFilter;
import com.alibaba.rocketmq.common.message.MessageExt;

public class MessageFilterImpl implements MessageFilter {

    @Override
    public boolean match(MessageExt msg) {
    	//no chinaese
    	//and we can user database query to filter message
        String property = msg.getUserProperty("SequenceId");
        if (property != null) {
            int id = Integer.parseInt(property);
            if ((id % 3) == 0) {
            	return true;
            }
        }

        return false;
    }
}
