/**
 * Created by ybwang on 3/1/17.
 */
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestIp {
    public static void main(String[] args) throws UnknownHostException {
        double[][] A = new double[][]{{1,0,1},{0,1,1},{1,0,0}};
        double[][] B = new double[][]{{1,1,1},{0,0,1},{0,1,0}};
        double[][] C = new double[A.length][B[0].length];
//        if(A[0].length!=B.length)
//            LOG.error("Input matrix size unmatched");
        for(int i=0;i<A.length;i++){
            for(int j=0;j<B[0].length;j++){
                for(int k=0;k<B.length;k++){
                    C[i][j]+=A[i][k]*B[k][j];
                }
            }
        }
        System.out.println(Arrays.deepToString(C));
    }
}
