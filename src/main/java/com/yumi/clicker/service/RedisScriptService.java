package com.yumi.clicker.service;

import com.yumi.clicker.common.CacheKeyConstants;
import com.yumi.clicker.common.TimeUtil;
import com.yumi.clicker.enums.ClickTypeEnum;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;

@Service
public class RedisScriptService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    public void addToRedisCacheSet(Long clickerId, Long receiverId, ClickTypeEnum clickTypeEnum) {
        String cacheKey = String.format(CacheKeyConstants.LIKE_RECORDS_KEY_TEMPLATE,
                clickerId, TimeUtil.currentDayStr());

        String script = "local set_key = KEYS[1]\n" +
                "local element = ARGV[1]\n" +
                "local max_size = tonumber(ARGV[3])\n" +
                "redis.call('SREM', set_key, ARGV[2])" +
                "redis.call('SADD', set_key, element)\n" +
                "local set_size = redis.call('SCARD', set_key)\n" +
                "if set_size == 1 then\n" +
                "    redis.call(\"EXPIRE\", set_key, 90000)\n" +
                "end\n" +
                "if set_size > max_size then\n" +
                "    local elements_to_remove = math.floor(set_size / 2)\n" +
                "    local elements = redis.call('SMEMBERS', set_key)\n" +
                "    for i = 1, elements_to_remove do\n" +
                "        redis.call('SREM', set_key, elements[i])\n" +
                "    end\n" +
                "end\n";

        String useStrReceiverId = clickTypeEnum.equals(ClickTypeEnum.LIKE) ?
                "+" + receiverId : "-" + receiverId;
        String notUseStrReceiverId = clickTypeEnum.equals(ClickTypeEnum.LIKE) ?
                "-" + receiverId : "+" + receiverId;

        stringRedisTemplate.execute(RedisScript.of(script), Collections.singletonList(cacheKey),
                useStrReceiverId, notUseStrReceiverId, "100");
    }
}
