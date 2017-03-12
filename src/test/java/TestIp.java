/**
 * Created by ybwang on 3/1/17.
 */
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class TestIp {
    public static void main(String[] args) throws UnknownHostException {
        String a=  " - Edgecut: 0, communication volume: 0.";
        String[] a1 = a.split(",")[0].split(" ");
        String a2 = a1[a1.length-1];
        System.out.println(a2);
    }
}
