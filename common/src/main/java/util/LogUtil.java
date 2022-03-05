package util;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author gusu
 * @date 2021/1/7
 */
public class LogUtil {
    private final static SimpleDateFormat dateFormat= new SimpleDateFormat("yyyy-MM-dd :hh:mm:ss");
    public static void log(Object s) {
        /* 获取当前调用log方法的类 */
        // 通过当前线程的堆栈信息来获取调用method
        final StackTraceElement stackTrace = Thread.currentThread().getStackTrace()[2];
        System.out.print("[INFO] " + dateFormat.format(new Date()) + "\t");
        System.out.print(stackTrace.getClassName()+"."+stackTrace.getMethodName()+"\t");
        System.out.println(s);
    }
}
