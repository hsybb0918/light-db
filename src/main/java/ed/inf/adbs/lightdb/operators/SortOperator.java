package ed.inf.adbs.lightdb.operators;

import ed.inf.adbs.lightdb.models.Tuple;
import ed.inf.adbs.lightdb.tools.CustomComparator;
import net.sf.jsqlparser.statement.select.OrderByElement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @ClassName: SortOperator
 * @Date: 14 March, 2021
 * @Author: Cyan
 */
public class SortOperator extends Operator {
    private Operator child;
    private List<Tuple> tuples;
    private List<String> orderColumns;

    private Tuple tuple;
    private int index = 0;

    public SortOperator(List<OrderByElement> orderByElements, Operator child) {
        this.child = child;
        this.tuples = new ArrayList<>();
        this.orderColumns = new ArrayList<>();

        while ((tuple = child.getNextTuple()) != null) {
            this.tuples.add(tuple);
        }

        for (OrderByElement orderByElement : orderByElements){
            this.orderColumns.add(orderByElement.toString());
        }

        Collections.sort(tuples, new CustomComparator(orderColumns));
    }

    @Override
    public Tuple getNextTuple() {
        if (index >= tuples.size()) {
            return null;
        } else {
            return tuples.get(index++);
        }
    }

    @Override
    public void reset() {
        index = 0;
    }
}
