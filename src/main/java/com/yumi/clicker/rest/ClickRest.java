package com.yumi.clicker.rest;

import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.yumi.clicker.common.CacheKeyConstants;
import com.yumi.clicker.common.TimeUtil;
import com.yumi.clicker.dto.ClickCommand;
import com.yumi.clicker.dto.ClickResult;
import com.yumi.clicker.dto.ViewResult;
import com.yumi.clicker.enums.ClickTypeEnum;
import com.yumi.clicker.mapper.ClickRecordsMapper;
import com.yumi.clicker.mapper.ClickSummaryMapper;
import com.yumi.clicker.po.ClickRecords;
import com.yumi.clicker.po.ClickSummary;
import com.yumi.clicker.service.RedisScriptService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.yumi.clicker.common.CacheKeyConstants.CNT_TEMPLATE;

@RestController
@RequestMapping("/click")
@Slf4j
public class ClickRest {
    @Value("${rocketmq.producer.group}")
    private String group;
    @Value("${rocketmq.user.click.topic}")
    private String topic;
    @Value("${rocketmq.name-server}")
    private String nameServerAddr;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private ClickSummaryMapper clickSummaryMapper;
    @Resource
    private ClickRecordsMapper clickRecordsMapper;
    @Resource
    private RedisScriptService redisScriptService;
    @Resource
    private Cache<Long, Long> clickSummaryCache;
    @Resource
    private Executor taskExecutor;
    private DefaultMQProducer producer;

    @PostConstruct
    public void init() throws MQClientException {
        producer = new DefaultMQProducer(this.group + "-" + this.getClass().getSimpleName());
        producer.setNamesrvAddr(nameServerAddr);
        producer.start();
    }

    @PreDestroy
    public void destroy() {
        if (null != producer) {
            producer.shutdown();
        }
    }

    @GetMapping("/{clickerId}/{receiverId}")
    public ViewResult view(@PathVariable Long clickerId, @PathVariable Long receiverId) throws ExecutionException,
            InterruptedException, TimeoutException {
        log.info("clickerId: {}, receiverId: {}", clickerId, receiverId);
        CompletableFuture<Long> cntFuture = CompletableFuture.supplyAsync(() -> {
            Long cacheCnt = clickSummaryCache.getIfPresent(receiverId);
            if (null != cacheCnt) {
                return cacheCnt;
            }
            String cntKey = String.format(CNT_TEMPLATE, receiverId);
            String strCntRes = stringRedisTemplate.opsForValue().get(cntKey);
            if (StringUtils.isNotBlank(strCntRes)) {
                Long cnt = Long.valueOf(strCntRes);
                clickSummaryCache.put(receiverId, cnt);
                return cnt;
            }
            LambdaQueryWrapper<ClickSummary> wrapper = new LambdaQueryWrapper<>();
            wrapper.eq(ClickSummary::getReceiverId, receiverId);
            ClickSummary clickSummary = clickSummaryMapper.selectOne(wrapper);
            if (null != clickSummary) {
                stringRedisTemplate.opsForValue().set(cntKey, clickSummary.getLikeCount().toString(),
                        1, TimeUnit.DAYS);
                clickSummaryCache.put(receiverId, clickSummary.getLikeCount());
                return clickSummary.getLikeCount();
            }
            clickSummaryCache.put(receiverId, 0L);
            return 0L;
        }, taskExecutor);


        CompletableFuture<Boolean> clickedFuture = CompletableFuture.supplyAsync(() -> {
            String cacheKey = String.format(CacheKeyConstants.LIKE_RECORDS_KEY_TEMPLATE,
                    clickerId, TimeUtil.currentDayStr());
            Boolean like = stringRedisTemplate.opsForSet().isMember(cacheKey, "+" + receiverId);
            if (Boolean.TRUE.equals(like)) {
                return true;
            }
            Boolean unlike = stringRedisTemplate.opsForSet().isMember(cacheKey, "-" + receiverId);
            if (Boolean.TRUE.equals(unlike)) {
                return false;
            }
            LambdaQueryWrapper<ClickRecords> wrapper = new LambdaQueryWrapper<>();
            wrapper.eq(ClickRecords::getReceiverId, receiverId);
            wrapper.eq(ClickRecords::getClickerId, clickerId);
            wrapper.orderByDesc(ClickRecords::getCreated);
            ClickRecords clickRecords = clickRecordsMapper.selectOne(wrapper);

            if (null == clickRecords || clickRecords.getClickType().equals(ClickTypeEnum.UNLIKE)) {
                redisScriptService.addToRedisCacheSet(clickerId, receiverId, ClickTypeEnum.UNLIKE);
                return false;
            }
            redisScriptService.addToRedisCacheSet(clickerId, receiverId, ClickTypeEnum.LIKE);
            return true;
        }, taskExecutor);

        CompletableFuture<ViewResult> viewResultCompletableFuture = cntFuture.thenCombine(clickedFuture, (cnt, clicked) -> {
            ViewResult viewResult = new ViewResult();
            viewResult.setClicked(clicked);
            viewResult.setCnt(cnt);
            return viewResult;
        });

        return viewResultCompletableFuture.get(1, TimeUnit.SECONDS);
    }

    @PostMapping
    public ClickResult click(@RequestBody ClickCommand clickCommand) {
        log.info("clickCommand: {}", JSON.toJSONString(clickCommand));
        ClickResult clickResult = new ClickResult();
        clickResult.setClicked(true);
        try {
            Message msg = new Message(topic, JSON.toJSONString(clickCommand).getBytes());
            SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    Long id = (Long) arg;
                    int index = id.intValue() % mqs.size();
                    return mqs.get(index);
                }
            }, clickCommand.getClickerId());

            if (sendResult.getSendStatus() != SendStatus.SEND_OK) {
                clickResult.setClicked(false);
            }

        } catch (Exception e) {
            log.error("click failed", e);
            clickResult.setClicked(false);
        }
        return clickResult;
    }
}
