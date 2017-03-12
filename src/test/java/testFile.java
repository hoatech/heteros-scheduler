import java.io.File;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by ybwang on 3/7/17.
 */
public class testFile {
    public static void main(String[] args){
        List<MyClass> list = new LinkedList<>();
        MyClass A = new MyClass(4, "A");
        MyClass B = new MyClass(4, "B");
        list.add(A);
        list.add(B);
        for(int i=0;i<8;i++) {
            Collections.sort(list);
            System.out.println(list.get(list.size()-1));
            list.get(list.size()-1).value--;
        }
    }
    static class MyClass implements Comparable<MyClass>{

        String name;
        int value;
        public MyClass(int value, String name){this.value = value;
        this.name = name;
        }

        @Override
        public int compareTo(MyClass o) {
            if(o.value<this.value)
                return 1;
            else if(o.value>this.value)
                return -1;
            return 0;
        }
        @Override
        public String toString(){
            return this.name+":"+this.value;
        }

    }
}
