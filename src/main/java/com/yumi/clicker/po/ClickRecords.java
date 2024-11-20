package com.yumi.clicker.po;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.yumi.clicker.enums.ClickTypeEnum;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@TableName("click_records")
public class ClickRecords {
    @TableId(value = "id", type = IdType.ASSIGN_ID)
    private Long id;
    private Long receiverId;
    private Long clickerId;
    private ClickTypeEnum clickType;
    private LocalDateTime created;
}
