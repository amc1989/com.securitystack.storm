package common;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by zhulei on 2017/4/6.
 */
public class DateFmt {

    public static final  String date_long="yyyy-MM-dd HH:mm:ss";
    public  static final String date_short="yyyy-MM-dd";
    public static SimpleDateFormat sdf = new SimpleDateFormat(date_short);

    public  static String getCountDate(String date,String patton) {
        SimpleDateFormat sdf = new SimpleDateFormat(patton);
        Calendar cal = Calendar.getInstance();
        if (null != date) {

            try {
                cal.setTime(sdf.parse(date));
            } catch (ParseException e) {
                e.printStackTrace();
            }

        }
        return sdf.format(cal.getTime());
    }

    public static Date parseDate(String date) throws ParseException {
        return sdf.parse(date);
    }
}
