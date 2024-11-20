package com.yumi.clicker.listener;

import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.yumi.clicker.common.NextIdUtil;
import com.yumi.clicker.mapper.ClickSummaryMapper;
import com.yumi.clicker.po.ClickRecords;
import com.yumi.clicker.po.ClickSummary;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.yumi.clicker.common.CacheKeyConstants.CNT_RECORD_TEMPLATE;
import static com.yumi.clicker.common.CacheKeyConstants.CNT_TEMPLATE;
import static com.yumi.clicker.common.CacheKeyConstants.LOCK_TEMPLATE;

@Service
@Slf4j
public class ClickSummaryListener implements MessageListenerOrderly {
    @Value("${rocketmq.click.summary.topic}")
    private String clickSummaryTopic;
    @Value("${rocketmq.consumer.group}")
    private String consumerGroup;
    @Value("${rocketmq.name-server}")
    private String nameServerAddr;

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private TransactionTemplate transactionTemplate;
    @Resource
    private ClickSummaryMapper clickSummaryMapper;

    private DefaultMQPushConsumer consumer;

    @PostConstruct
    public void init() throws MQClientException {
        consumer = new DefaultMQPushConsumer(consumerGroup + "-" + this.getClass().getSimpleName());
        consumer.setNamesrvAddr(nameServerAddr);
        consumer.subscribe(this.clickSummaryTopic, "*");
        consumer.registerMessageListener(this);
        consumer.start();
    }

    @PreDestroy
    public void destroy() {
        if (null != consumer) {
            consumer.shutdown();
        }
    }

    @Override
    public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
        context.setAutoCommit(true);
        try {
            for (MessageExt msg : msgs) {
                String strBody = new String(msg.getBody());
                log.info("strBody: {}", strBody);
                ClickRecords clickRecords = JSON.parseObject(strBody, ClickRecords.class);
                String lockKey = String.format(LOCK_TEMPLATE, clickRecords.getReceiverId());
                String cntRecordKey = String.format(CNT_RECORD_TEMPLATE, clickRecords.getReceiverId(),
                        clickRecords.getId());
                String script = "local key = KEYS[1]\n" +
                        "local idToUse = redis.call(\"GET\", key)\n" +
                        "if ARGV[1] == idToUse then\n" +
                        "    redis.call(\"DEL\", key)\n" +
                        "end";
                stringRedisTemplate.execute(RedisScript.of(script),
                        Collections.singletonList(lockKey), clickRecords.getId().toString());
                String strCnt = stringRedisTemplate.opsForValue().get(cntRecordKey);
                log.info("strCnt: {}", strCnt);
                if (null == strCnt) {
                    continue;
                }
                boolean res = Boolean.TRUE.equals(transactionTemplate.execute(action -> {
                    try {
                        LambdaQueryWrapper<ClickSummary> wrapper = new LambdaQueryWrapper<>();
                        wrapper.eq(ClickSummary::getReceiverId, clickRecords.getReceiverId());
                        ClickSummary clickSummary = clickSummaryMapper.selectOne(wrapper);
                        long cntToAdd = Long.valueOf(strCnt);
                        if (null != clickSummary) {
                            clickSummary.setLikeCount(clickSummary.getLikeCount() + cntToAdd);
                            clickSummary.setModified(LocalDateTime.now());
                            clickSummaryMapper.updateById(clickSummary);
                        } else {
                            clickSummary = new ClickSummary();
                            clickSummary.setId(NextIdUtil.nextId());
                            clickSummary.setCreated(LocalDateTime.now());
                            clickSummary.setModified(LocalDateTime.now());
                            clickSummary.setReceiverId(clickRecords.getReceiverId());
                            clickSummary.setLikeCount(cntToAdd);
                            clickSummaryMapper.insert(clickSummary);
                        }
                        String cntKey = String.format(CNT_TEMPLATE, clickSummary.getReceiverId());
                        stringRedisTemplate.opsForValue().set(cntKey, clickSummary.getLikeCount().toString(), 3, TimeUnit.DAYS);
                        stringRedisTemplate.delete(cntRecordKey);
                        return true;
                    } catch (Exception e) {
                        action.setRollbackOnly();
                        return false;
                    }
                }));

                if (res) {
                    return ConsumeOrderlyStatus.SUCCESS;
                } else {
                    return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
        }
        return ConsumeOrderlyStatus.SUCCESS;
    }
}
