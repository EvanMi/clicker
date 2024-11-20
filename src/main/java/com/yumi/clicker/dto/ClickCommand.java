package com.yumi.clicker.dto;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ClickCommand {
    private Long clickerId;
    private Long receiverId;
    private Integer clickType;
}
