import java.util.ArrayList;

public class testClass {

    public static void doTheThing(ArrayList<Integer> ...lists) {
        int count = 0;
        for(ArrayList<Integer> list : lists){
            count +=1;
            System.out.println("List #" + count);

            for(Integer item : list){

            }

        }
    }
    public static void main(String[] args) {

        ArrayList<ArrayList<Integer>> tostaki = new ArrayList<ArrayList<Integer>>();
        ArrayList<Integer> t1 = new ArrayList<>();
        t1.add(1);
        t1.add(2);
        ArrayList<Integer> t2 = new ArrayList<>();
        t2.add(1);
        t2.add(2);

        tostaki.add(t1);
        tostaki.add(t2);

        doTheThing(tostaki.toArray(new ArrayList[0]));
    }

}
