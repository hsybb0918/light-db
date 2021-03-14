package ed.inf.adbs.lightdb.models;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Handle tuples as objects.
 *
 * @ClassName: Tuple
 * @Date: 12 March, 2021
 * @Author: Cyan
 */
public class Tuple {

    private Map<String, Integer> tupleMap = new LinkedHashMap<>();

    /**
     * Initialize the tuple.
     *  @param values values of corresponding field names
     * @param fields field names
     */
    public Tuple(List<Integer> values, List<String> fields) {
        for (int i = 0; i < values.size(); i++) {
            this.tupleMap.put(fields.get(i), values.get(i));
        }
    }

    public Map<String, Integer> getTupleMap() {
        return tupleMap;
    }

    /**
     * Get the tuple values as a string, separate with comma.
     *
     * @return tuple as string
     */
    public String getTupleString() {
        StringBuilder sb = new StringBuilder();

        int count = 0;
        for (Integer i : tupleMap.values()) {
            sb.append(i);
            if (count < tupleMap.size() - 1) {
                count++;
                sb.append(",");
            }
        }

        return sb.toString();
    }

}
