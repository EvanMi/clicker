package com.yumi.clicker.listener;

import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.yumi.clicker.common.CacheKeyConstants;
import com.yumi.clicker.common.CodeDescEnum;
import com.yumi.clicker.common.NextIdUtil;
import com.yumi.clicker.common.TimeUtil;
import com.yumi.clicker.dto.ClickCommand;
import com.yumi.clicker.enums.ClickTypeEnum;
import com.yumi.clicker.mapper.ClickRecordsMapper;
import com.yumi.clicker.po.ClickRecords;
import com.yumi.clicker.service.RedisScriptService;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;
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

import static org.apache.rocketmq.client.producer.SendStatus.SEND_OK;

@Service
@Slf4j
public class UserClickListener implements MessageListenerOrderly {
    @Resource
    private Cache<String, ClickTypeEnum> userClickRecordsCache;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private ClickRecordsMapper clickRecordsMapper;
    @Resource
    private TransactionTemplate transactionTemplate;
    @Value("${rocketmq.user.click.topic}")
    private String userClickTopic;
    @Value("${rocketmq.user.click.record.topic}")
    private String userClickRecordTopic;
    @Value("${rocketmq.consumer.group}")
    private String consumerGroup;
    @Value("${rocketmq.producer.group}")
    private String group;
    @Value("${rocketmq.name-server}")
    private String nameServerAddr;
    @Resource
    private RedisScriptService redisScriptService;
    private DefaultMQPushConsumer consumer;
    private TransactionMQProducer producer;

    @PostConstruct
    public void init() throws MQClientException {
        producer = new TransactionMQProducer(group + "-" + this.getClass().getSimpleName());
        producer.setNamesrvAddr(nameServerAddr);
        producer.setTransactionListener(new TransactionListenerLocal());
        producer.start();

        consumer = new DefaultMQPushConsumer(consumerGroup + "-" + this.getClass().getSimpleName());
        consumer.setNamesrvAddr(nameServerAddr);
        consumer.subscribe(this.userClickTopic, "*");
        consumer.registerMessageListener(this);
        consumer.start();
    }

    @PreDestroy
    public void destroy() {
        if (null != producer) {
            producer.shutdown();
        }
        if (null != consumer) {
            consumer.shutdown();
        }
    }

    @Override
    public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext context) {
        context.setAutoCommit(true);
        try {
            for (MessageExt message : list) {
                String msg = new String(message.getBody());
                log.info("msg: {}", msg);
                ClickCommand clickCommand = JSON.parseObject(msg, ClickCommand.class);
                ClickTypeEnum clickType = CodeDescEnum.parseFromCode(ClickTypeEnum.class, clickCommand.getClickType());
                String cacheKey = String.format(CacheKeyConstants.LIKE_RECORDS_KEY_TEMPLATE,
                        clickCommand.getClickerId(), TimeUtil.currentDayStr());
                log.info("cacheKey: {}", cacheKey);
                String localCacheKey = cacheKey + "#" + clickCommand.getReceiverId();
                ClickTypeEnum lockCacheVar = userClickRecordsCache.getIfPresent(localCacheKey);
                if (clickType.equals(lockCacheVar)) {
                    //本地缓存命中，直接返回
                    log.info("hit local cache {}", lockCacheVar.name());
                    continue;
                }
                Boolean redisCacheVar = stringRedisTemplate.opsForSet().isMember(cacheKey, clickType.equals(ClickTypeEnum.LIKE) ?
                        "+" + clickCommand.getReceiverId() : "-" + clickCommand.getReceiverId());
                if (null != redisCacheVar && redisCacheVar) {
                    //redis 缓存命中
                    userClickRecordsCache.put(localCacheKey, clickType);
                    log.info("hit redis {}", clickType.name());
                    continue;
                }
                LambdaQueryWrapper<ClickRecords> wrapper = new LambdaQueryWrapper<>();
                wrapper.eq(ClickRecords::getReceiverId, clickCommand.getReceiverId());
                wrapper.eq(ClickRecords::getClickerId, clickCommand.getClickerId());
                wrapper.orderByDesc(ClickRecords::getCreated);

                ClickRecords clickRecords = clickRecordsMapper.selectOne(wrapper);
                if (null != clickRecords) {
                    //更新缓存
                    userClickRecordsCache.put(localCacheKey, clickRecords.getClickType());
                    redisScriptService.addToRedisCacheSet(clickCommand.getClickerId(), clickCommand.getReceiverId(), clickRecords.getClickType());
                    if (clickType.equals(clickRecords.getClickType())) {
                        //数据库命中
                        log.info("hit db {}", JSON.toJSONString(clickRecords));
                        continue;
                    }
                } else {
                    if (clickType.equals(ClickTypeEnum.UNLIKE)) {
                        //没有点赞过不能取消点赞
                        log.info("null hit db type: {}", clickType);
                        userClickRecordsCache.put(localCacheKey, clickType);
                        redisScriptService.addToRedisCacheSet(clickCommand.getClickerId(), clickCommand.getReceiverId(), ClickTypeEnum.UNLIKE);
                        continue;
                    }
                }

                Long nextId = NextIdUtil.nextId();
                ClickRecords recordToInsert = new ClickRecords();
                recordToInsert.setId(nextId);
                recordToInsert.setClickType(clickType);
                recordToInsert.setClickerId(clickCommand.getClickerId());
                recordToInsert.setReceiverId(clickCommand.getReceiverId());
                recordToInsert.setCreated(LocalDateTime.now());
                Message clickRecordMsg =
                        new Message(userClickRecordTopic, JSON.toJSONString(recordToInsert)
                                .getBytes(RemotingHelper.DEFAULT_CHARSET));
                SendResult sendResult = producer.sendMessageInTransaction(clickRecordMsg, null);
                log.info("click record msg sent, sendResult: {} msg is {}", sendResult, JSON.toJSONString(clickRecordMsg));
                if (!sendResult.getSendStatus().equals(SEND_OK)) {
                    return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
        }

        return ConsumeOrderlyStatus.SUCCESS;
    }




    private class TransactionListenerLocal implements TransactionListener {
        @Override
        public LocalTransactionState checkLocalTransaction(MessageExt msg) {
            String strBody = new String(msg.getBody());
            log.info("strBody: {}", strBody);
            ClickRecords clickRecords = JSON.parseObject(strBody, ClickRecords.class);
            ClickRecords foundClickRecords = clickRecordsMapper.selectById(clickRecords.getId());
            if (null != foundClickRecords) {
                log.info("msg committed to mq");
                return LocalTransactionState.COMMIT_MESSAGE;
            } else {
                if (clickRecords.getCreated().plusSeconds(20).isAfter(LocalDateTime.now())) {
                    log.info("waiting... and waiting...");
                    return LocalTransactionState.UNKNOW;
                }
                log.info("msg rollback to mq");
                return LocalTransactionState.ROLLBACK_MESSAGE;
            }
        }
        @Override
        public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
            // 执行本地事务逻辑
            ClickRecords recordToInsert = JSON.parseObject(new String(msg.getBody()), ClickRecords.class);
            try {
                String cacheKey = String.format(CacheKeyConstants.LIKE_RECORDS_KEY_TEMPLATE,
                        recordToInsert.getClickerId(), TimeUtil.currentDayStr());
                String localCacheKey = cacheKey + "#" + recordToInsert.getReceiverId();
                clickRecordsMapper.insert(recordToInsert);
                log.info("cacheKey: {} is inserted to db", cacheKey);
                redisScriptService.addToRedisCacheSet(recordToInsert.getClickerId(), recordToInsert.getReceiverId(),
                        recordToInsert.getClickType());
                log.info("cacheKey: {} is inserted to redis", cacheKey);
                userClickRecordsCache.put(localCacheKey, recordToInsert.getClickType());
                log.info("cacheKey: {} is inserted to local cache", cacheKey);

                return LocalTransactionState.COMMIT_MESSAGE;
            } catch (Exception e) {
                e.printStackTrace();
                return LocalTransactionState.UNKNOW;
            }
        }
    }
}
