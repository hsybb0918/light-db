package ed.inf.adbs.lightdb;

import ed.inf.adbs.lightdb.models.Tuple;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ClassName: Test
 * @Date: 12 March, 2021
 * @Author: Cyan
 */
public class Test {
    public static void main(String[] args) {
//        HashMap<String, String> map = new HashMap<String, String>();
//        map.put("1", "11");
//        map.put("2", "22");
//        for (String i : map.values()) {
//            System.out.println(i);
//        }

//        int[] values = {1, 2, 3};
//        String[] fields = {"A", "B", "C"};
//        Tuple tuple = new Tuple(values, fields, "table");
//        System.out.println(tuple.getTupleString());

//        StringBuilder sb = new StringBuilder();
//        System.out.println(sb);

        Map<String, String> map= new HashMap<>();
        map.put("a", "b");
        map.put("c", "d");
        System.out.println(map.get(-1));
    }

}
