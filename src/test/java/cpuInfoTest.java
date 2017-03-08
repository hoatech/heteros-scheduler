import edu.fudan.storm.LoadMonitor.LoadMonitor;
import org.apache.storm.grouping.Load;
import org.apache.storm.shade.org.apache.curator.utils.ThreadUtils;
import org.apache.storm.transactional.state.TestTransactionalState;

import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * Created by ybwang on 2/22/17.
 */
public class cpuInfoTest {
    static Random rand = new Random();

    public static void main(String[] args) throws InterruptedException {
        class testThread implements Runnable{
            @Override
            public void run() {
                double a = rand.nextDouble();
                while(true)
                    a=a*a;
            }
        }
        LoadMonitor monitor = LoadMonitor.getInstance();
        Thread thread1 = new Thread(new testThread());
        Thread thread2 = new Thread(new testThread());
        Thread thread3 = new Thread(new testThread());
        Thread thread4 = new Thread(new testThread());
        thread1.start();
        thread2.start();
        thread3.start();
        thread4.start();
        Set<Long> threadSet = new HashSet();
        threadSet.add(thread1.getId());
        threadSet.add(thread2.getId());
        threadSet.add(thread3.getId());
        threadSet.add(thread4.getId());
        for(int i=0;i<5;i++) {
            Map<Long, Long> loadInfo = monitor.getLoadInfo(threadSet);
            for(long id : loadInfo.keySet()){
                System.out.println("Thread "+id +" consume "+loadInfo.get(id)+" cycles per second");
            }
            Thread.sleep(1000);
        }
    }
}
