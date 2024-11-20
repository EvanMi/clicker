package com.yumi.clicker.enums;

import com.yumi.clicker.common.CodeDescEnum;

public enum ClickTypeEnum implements CodeDescEnum<ClickTypeEnum> {
    LIKE(1, "点赞"),
    UNLIKE(-1, "取消点赞");

    ClickTypeEnum(Integer code, String desc) {
        addCodeDesc(code, desc);
    }

    @Override
    public ClickTypeEnum self() {
        return this;
    }
}
