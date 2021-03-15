package ed.inf.adbs.lightdb.operators;

import ed.inf.adbs.lightdb.models.Tuple;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.ArrayList;
import java.util.List;

/**
 * Has one child, projection on part of columns.
 *
 * ClassName: ProjectOperator
 * Date: 13 March, 2021
 * Author: Cyan
 */
public class ProjectOperator extends Operator{
    private Operator child;

    private Tuple tuple;
    private List<String> selectedColumns;

    /**
     * Constructor: init the selected columns list.
     *
     * @param selectItems select columns
     * @param child child operator
     */
    public ProjectOperator(List<SelectItem> selectItems, Operator child) {
        this.child = child;

        this.selectedColumns = new ArrayList<>();
        for (SelectItem selectItem : selectItems) {
            // only consider SelectExpressionItem
            // if AllColumns, will not create the project operator
            selectedColumns.add(selectItem.toString());
        }
    }

    /**
     * Get next tuple with selected columns.
     *
     * @return tuple with selected columns
     */
    @Override
    public Tuple getNextTuple() {
        while ((tuple = child.getNextTuple()) != null) {
            List<Integer> tupleValues = new ArrayList<>();
            for (String column : selectedColumns) { // add values if need this column
                tupleValues.add(tuple.getTupleMap().get(column));
            }

            return new Tuple(tupleValues, selectedColumns);
        }
        return null;
    }

    /**
     * Use function from child.
     */
    @Override
    public void reset() {
        child.reset();
    }
}
