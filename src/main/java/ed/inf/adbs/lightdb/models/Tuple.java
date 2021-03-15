package ed.inf.adbs.lightdb.models;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Handle tuples as objects.
 *
 * ClassName: Tuple
 * Date: 12 March, 2021
 * Author: Cyan
 */
public class Tuple {
    private Map<String, Integer> tupleMap;

    /**
     * Constructor: initialize the tuple map.
     *
     * @param values values of corresponding column names
     * @param column column names
     */
    public Tuple(List<Integer> values, List<String> column) {
        // use linked hashmap in order to make it in order
        this.tupleMap = new LinkedHashMap<>();

        // put together as a map
        for (int i = 0; i < values.size(); i++) {
            this.tupleMap.put(column.get(i), values.get(i));
        }
    }

    /**
     * Getter: get the member map.
     *
     * @return tuple map
     */
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

            // add comma between columns
            if (count < tupleMap.size() - 1) {
                count++;
                sb.append(",");
            }
        }

        return sb.toString();
    }
}
