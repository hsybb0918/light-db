package ed.inf.adbs.lightdb.operators;

import ed.inf.adbs.lightdb.models.Tuple;
import ed.inf.adbs.lightdb.tools.TupleComparator;
import net.sf.jsqlparser.statement.select.OrderByElement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Has one child, a blocking operator which sorts the tuples on required columns.
 *
 * ClassName: SortOperator
 * Date: 14 March, 2021
 * Author: Cyan
 */
public class SortOperator extends Operator {
    private Operator child;
    private List<Tuple> tuples;
    private List<String> orderColumns;

    private Tuple tuple;
    private int index;

    /**
     * Constructor: buffer all tuples and init required sorting columns.
     *
     * @param orderByElements ordered columns
     * @param child child operator
     */
    public SortOperator(List<OrderByElement> orderByElements, Operator child) {
        this.child = child;
        this.tuples = new ArrayList<>();
        this.orderColumns = new ArrayList<>();

        // buffer all tuples
        while ((tuple = child.getNextTuple()) != null) {
            this.tuples.add(tuple);
        }

        // get all required sorting columns
        for (OrderByElement orderByElement : orderByElements){
            this.orderColumns.add(orderByElement.toString());
        }

        // directly sort according to the comparator
        Collections.sort(tuples, new TupleComparator(orderColumns));
    }

    /**
     * Iteratively get the sorted tuples.
     *
     * @return tuple object
     */
    @Override
    public Tuple getNextTuple() {
        if (index >= tuples.size()) {
            return null;
        } else {
            return tuples.get(index++); // return if still have tuples, and advance index
        }
    }

    /**
     * Reset the index and use child reset function
     */
    @Override
    public void reset() {
        index = 0;
        child.reset();
    }
}
