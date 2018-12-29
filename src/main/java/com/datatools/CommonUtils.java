package com.datatools;

import java.text.SimpleDateFormat;
import java.util.Date;

public class CommonUtils {

    /**
     * 用时间生成ID
     * @return
     */
    public static String createID() {
        String time = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        return time;
    }

}
