package com.yumi.clicker.common;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class TimeUtil {
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private TimeUtil() {}

    public static String currentDayStr() {
        return DATE_FORMATTER.format(LocalDate.now());
    }
}
