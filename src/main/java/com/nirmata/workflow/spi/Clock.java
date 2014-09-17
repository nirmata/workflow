package com.nirmata.workflow.spi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

public class Clock
{
    private static final Logger log = LoggerFactory.getLogger(Clock.class);
    private static final TimeZone utc = TimeZone.getTimeZone("UTC");

    /**
     * Return the current date/time UTC
     *
     * @return current date/time UTC
     */
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

    /**
     * Convert a date to a string in iso-8601 format
     *
     * @param date the date
     * @return string
     */
    public static String dateToString(Date date)
    {
        return newIsoDateFormatter().format(date);
    }

    /**
     * Parse an iso-8601 string into a date
     *
     * @param str iso-8601 string
     * @return date
     */
    public static Date dateFromString(String str)
    {
        try
        {
            return newIsoDateFormatter().parse(str);
        }
        catch ( ParseException e )
        {
            log.error("parsing date: " + str, e);
            throw new RuntimeException(e);
        }
    }

    private static DateFormat newIsoDateFormatter()
    {
        // per http://stackoverflow.com/questions/2201925/converting-iso-8601-compliant-string-to-java-util-date
        return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    }

    private Clock()
    {
    }
}
