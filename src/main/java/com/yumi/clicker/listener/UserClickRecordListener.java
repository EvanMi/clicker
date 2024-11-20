package com.yumi.clicker.listener;

import com.alibaba.fastjson.JSON;
import com.yumi.clicker.po.ClickRecords;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.yumi.clicker.common.CacheKeyConstants.CNT_RECORD_TEMPLATE;
import static com.yumi.clicker.common.CacheKeyConstants.LOCK_TEMPLATE;

@Service
@Slf4j
public class UserClickRecordListener implements MessageListenerConcurrently {

    @Value("${rocketmq.user.click.record.topic}")
    private String userClickRecordTopic;
    @Value("${rocketmq.consumer.group}")
    private String consumerGroup;
    @Value("${rocketmq.name-server}")
    private String nameServerAddr;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    private DefaultMQPushConsumer consumer;

    @Value("${rocketmq.click.summary.topic}")
    private String clickSummaryTopic;
    @Value("${rocketmq.producer.group}")
    private String group;
    private DefaultMQProducer producer;

    @PostConstruct
    public void init() throws MQClientException {
        producer = new DefaultMQProducer(this.group + "-" + this.getClass().getSimpleName());
        producer.setNamesrvAddr(nameServerAddr);
        producer.start();

        consumer = new DefaultMQPushConsumer(consumerGroup + "-" + this.getClass().getSimpleName());
        consumer.setNamesrvAddr(nameServerAddr);
        consumer.subscribe(this.userClickRecordTopic, "*");
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
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
        for (MessageExt msg : msgs) {
            String strBody = new String(msg.getBody());
            log.info("strBody: {}", strBody);
            ClickRecords clickRecords = JSON.parseObject(strBody, ClickRecords.class);
            String lockKey = String.format(LOCK_TEMPLATE, clickRecords.getReceiverId());
            String cntRecordKey = String.format(CNT_RECORD_TEMPLATE, clickRecords.getReceiverId(),
                    clickRecords.getId());
            String script = "local receiverId = ARGV[1]\n" +
                    "local recordId = ARGV[2]\n" +
                    "local lockKey = KEYS[1]\n" +
                    "local cntRecordKey = KEYS[2]\n" +
                    "local setnx_result = redis.call(\"SETNX\", lockKey, recordId)\n" +
                    "if setnx_result == 1 then\n" +
                    "    redis.call(\"INCRBY\", cntRecordKey, tonumber(ARGV[3]))\n" +
                    "    return recordId\n" +
                    "else\n" +
                    "    local idToUse = redis.call(\"GEt\", lockKey)\n" +
                    "    if idToUse == recordId then\n" +
                    "        return recordId\n" +
                    "    end\n" +
                    "    local cntKeyForIdToUse = \"cnt#{\" .. receiverId .. \"}#\" .. idToUse\n" +
                    "    redis.call(\"INCRBY\", cntKeyForIdToUse, tonumber(ARGV[3]))\n" +
                    "    return idToUse\n" +
                    "end";
            List<String> keys = new ArrayList<>();
            keys.add(lockKey);
            keys.add(cntRecordKey);
            String res = stringRedisTemplate.execute(RedisScript.of(script, String.class), keys,
                    clickRecords.getReceiverId().toString(), clickRecords.getId().toString(),
                    clickRecords.getClickType().getCode().toString());
            log.info("inId {}, usedId {}", clickRecords.getId(), res);
            if (null != res && Objects.equals(res, clickRecords.getId().toString())) {
                try {
                    Message newMsg = new Message(clickSummaryTopic, strBody.getBytes());
                    SendResult sendResult = producer.send(newMsg, new MessageQueueSelector() {
                        @Override
                        public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                            Long id = (Long) arg;
                            int index = id.intValue() % mqs.size();
                            return mqs.get(index);
                        }
                    }, clickRecords.getReceiverId());

                    if (sendResult.getSendStatus() != SendStatus.SEND_OK) {
                        return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                    }
                } catch (Exception e) {
                    log.error("summary failed", e);
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
            }
        }
        log.info("summary success");
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }
}
