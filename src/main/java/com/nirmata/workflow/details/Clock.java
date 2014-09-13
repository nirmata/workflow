package com.nirmata.workflow.details;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

public class Clock
{
    private static final TimeZone utc = TimeZone.getTimeZone("UTC");

    public static Date nowUtc()
    {
        Calendar calendar = Calendar.getInstance(utc);
        GregorianCalendar gregorianCalendar = new GregorianCalendar
        (
            calendar.get(Calendar.YEAR),
            calendar.get(Calendar.MONTH),
            calendar.get(Calendar.DAY_OF_MONTH),
            calendar.get(Calendar.HOUR_OF_DAY),
            calendar.get(Calendar.MINUTE),
            calendar.get(Calendar.SECOND)
        );
        return gregorianCalendar.getTime();
    }

    private Clock()
    {
    }
}
