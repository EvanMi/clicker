package com.yumi.clicker.po;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@TableName("click_summary")
public class ClickSummary {
    @TableId(value = "id", type = IdType.ASSIGN_ID)
    private Long id;
    private Long receiverId;
    private Long likeCount;
    private LocalDateTime created;
    private LocalDateTime modified;
}
