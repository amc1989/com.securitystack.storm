import sun.tools.jar.Main;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by zhulei on 2017/4/5.
 */
public class test {
    private Integer age;
    private String name;

    public static void main(String[] arges) {

        List<String> list = new ArrayList<String>() {
            {
                add("lucy");
                add("lily");
                add("mark");
                add("xiaoming");
            }
        };
        Iterator<String> iterator = list.iterator();
        for(String s:list){
            System.out.println("1    "+s);
        }
        for(String s:list){
            System.out.println("2    "+s);
        }

    }
}
