package ed.inf.adbs.lightdb.tools;

import ed.inf.adbs.lightdb.models.Tuple;

import java.util.Comparator;
import java.util.List;

/**
 * @ClassName: CustomComparator
 * @Date: 14 March, 2021
 * @Author: Cyan
 */
public class CustomComparator implements Comparator<Tuple> {
    private List<String> orderColumns;

    public CustomComparator(List<String> orderColumns) {
        this.orderColumns = orderColumns;
    }

    @Override
    public int compare(Tuple o1, Tuple o2) {

        for (String column : orderColumns) {
            if (o1.getTupleMap().get(column) < o2.getTupleMap().get(column)) {
                return -1;
            }
            if (o1.getTupleMap().get(column) > o2.getTupleMap().get(column)) {
                return 1;
            }
            // Just continue if the tuples match for the given order...
        }

        // Vikas check this.
        boolean isPresent = false;
        for (String key : o1.getTupleMap().keySet()) {
            for (int j = 0; j < orderColumns.size(); j++) {
                if (orderColumns.get(j) == key) {
                    isPresent = true;
                }
            }
            if (!isPresent) {
                if (o1.getTupleMap().get(key) < o2.getTupleMap().get(key))
                    return -1;
                if (o1.getTupleMap().get(key) > o2.getTupleMap().get(key))
                    return 1;
            }
        }

        return 0;
    }
}
