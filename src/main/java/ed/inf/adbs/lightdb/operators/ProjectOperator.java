package ed.inf.adbs.lightdb.operators;

import ed.inf.adbs.lightdb.models.Tuple;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName: ProjectOperator
 * @Date: 13 March, 2021
 * @Author: Cyan
 */
public class ProjectOperator extends Operator{
    private Operator child;
    private List<SelectItem> selectItems;

    private Tuple tuple;
    private List<String> selectedColumns;

    public ProjectOperator(List<SelectItem> selectItems, Operator child) {
        this.child = child;
        this.selectItems = selectItems;

        selectedColumns = new ArrayList<>();
        for (SelectItem selectItem : selectItems) { // only will be SelectExpressionItem
            selectedColumns.add(selectItem.toString());
        }
    }

    @Override
    public Tuple getNextTuple() {
        while ((tuple = child.getNextTuple()) != null) {
            List<Integer> tupleValues = new ArrayList<>();
            for (String column : selectedColumns) {
                tupleValues.add(tuple.getValueByColumn(column));
            }
            return new Tuple(tupleValues, selectedColumns);

        }
        return null;

//        Tuple childNextTuple = child.getNextTuple();
//        if (childNextTuple == null)
//            return childNextTuple;
//        int k = 0;
//        int[] projectedAttributes = new int[schema.size()];
//        for (String schemaCol : schema) {
//            long projectedColumnValue = AttributeMapper.getColumnActualValue(childNextTuple, child.getSchema(), schemaCol);
//            projectedAttributes[k++] = (int)projectedColumnValue;
//        }
//
//        return new Tuple(projectedAttributes);
    }

    @Override
    public void reset() {
        child.reset();
    }
}
