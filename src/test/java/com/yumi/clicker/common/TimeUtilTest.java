package com.yumi.clicker.common;

import com.yumi.clicker.enums.ClickTypeEnum;
import org.junit.jupiter.api.Test;

public class TimeUtilTest {

    @Test
    void testCurrentDayStr() {
        System.out.println(TimeUtil.currentDayStr());

        CodeDescEnum.parseFromCode(ClickTypeEnum.class, 1);
    }
}
