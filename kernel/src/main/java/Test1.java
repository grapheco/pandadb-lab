import java.util.ArrayList;
import java.util.concurrent.locks.LockSupport;


public class Test1 {

    public static void main(String[] args) {

        ArrayList<String> l1 = new ArrayList<String>();

        ArrayList<Integer> l2 = new ArrayList<Integer>();

        l1.add("1");

        l2.add(1);

        System.out.println(l1.get(0).getClass());

        System.out.println(l2.get(0).getClass());

        System.out.println(l1.getClass() == l2.getClass());

    }
//    public static void main(String[] args) {
//
//        LockSupport.unpark(Thread.currentThread());
//
//        LockSupport.park();
//
//        System.out.println("hello");
//
//        LockSupport.unpark(Thread.currentThread());
//
//        LockSupport.unpark(Thread.currentThread());
//
//        LockSupport.park();
//
//        LockSupport.park();
//
//        System.out.println("hello");
//
//    }

}