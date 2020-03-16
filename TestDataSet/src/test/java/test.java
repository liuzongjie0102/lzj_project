import java.util.ArrayList;
import java.util.List;

public class test {
    public static void main(String[] args){
        List<String> list = new ArrayList<>();
        list.add("a");
        System.out.println(list.contains(new String("a")));
    }
}
