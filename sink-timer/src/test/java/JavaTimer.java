import javafx.beans.binding.When;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

public class JavaTimer {

    public static void main(String[] args) throws InterruptedException {

//        Timer timer = new Timer();
//        //delay 延迟执行
//        //period 周期
//        //date 指定日期
//        timer.schedule(new TimerTask() {
//            @Override
//            public void run() {
//                System.out.println("时间到");
//            }
//        }, 0, 3000);//milliseconds

        TimeUnit.SECONDS.sleep(5);
        System.out.println("hahah");
    }
}
