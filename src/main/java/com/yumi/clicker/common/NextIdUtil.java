package com.yumi.clicker.common;

import com.baomidou.mybatisplus.core.incrementer.DefaultIdentifierGenerator;

public class NextIdUtil {
    public static long nextId() {
      return DefaultIdentifierGenerator.getInstance().nextId(null);
    }
}
