package com.yumi.clicker.common;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CacheKeyConstantsTest {


    @Test
    void testStringTemplate() {
        String likeRecordsKeyTemplate = CacheKeyConstants.LIKE_RECORDS_KEY_TEMPLATE;
        String format = String.format(likeRecordsKeyTemplate, "yumi", "2024-02-02");
        Assertions.assertEquals("like#records#{yumi}#{2024-02-02}", format);
    }
}
