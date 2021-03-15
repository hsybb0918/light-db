package ed.inf.adbs.lightdb.tools;

import ed.inf.adbs.lightdb.models.Tuple;

import java.util.Comparator;
import java.util.List;

/**
 * Comparator used to compare tuples when sorting.
 *
 * ClassName: CustomComparator
 * Date: 14 March, 2021
 * Author: Cyan
 */
public class TupleComparator implements Comparator<Tuple> {
    private List<String> orderColumns; // columns required to sorting

    /**
     * Constructor: init columns required to sorting
     *
     * @param orderColumns column names
     */
    public TupleComparator(List<String> orderColumns) {
        this.orderColumns = orderColumns;
    }

    /**
     * Rewrite the compare function used to compare the two tuples,
     * if more than one column, first sorting on one column, then another.
     *
     * @param o1 first tuple
     * @param o2 second tuple
     * @return 1 if larger than, or -1 if less than, or 0 if equal to
     */
    @Override
    public int compare(Tuple o1, Tuple o2) {
        for (String column : orderColumns) {
            if (o1.getTupleMap().get(column) < o2.getTupleMap().get(column)) {
                return -1;
            }
            if (o1.getTupleMap().get(column) > o2.getTupleMap().get(column)) {
                return 1;
            }
            // if continue, means the tuples match on the given ordered columns
        }

        return 0; // otherwise, two tuples are the same
    }
}
