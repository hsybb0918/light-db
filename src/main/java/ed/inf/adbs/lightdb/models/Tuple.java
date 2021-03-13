package ed.inf.adbs.lightdb.models;

import java.util.HashMap;
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
    private String tableName;
    private Map<String, Integer> fieldToValue = new HashMap<>();

    /**
     * Initialize the tuple.
     *  @param values values of corresponding field names
     * @param fields field names
     * @param tableName table name
     */
    public Tuple(List<Integer> values, List<String> fields, String tableName) {
        for (int i = 0; i < values.size(); i++) {
            this.fieldToValue.put(fields.get(i), values.get(i));
        }
        this.tableName = tableName;
    }

    /**
     * Get the size of the tuple.
     *
     * @return size of the tuple
     */
    public int getSize() {
        return fieldToValue.size();
    }

    /**
     * Get the tuple values as a string, separate with comma.
     *
     * @return tuple as string
     */
    public String getTupleString() {
        StringBuilder sb = new StringBuilder();

        int count = 0;
        for (Integer i : fieldToValue.values()) {
            sb.append(i);
            if (count < fieldToValue.size() - 1) {
                count++;
                sb.append(",");
            }
        }

        return sb.toString();
    }
}
