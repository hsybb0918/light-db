package ed.inf.adbs.lightdb.tools;

import ed.inf.adbs.lightdb.operators.*;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;

import java.io.BufferedReader;
import java.io.PrintStream;
import java.util.*;

/**
 * Generate the query plan and process from top to down
 *
 * @ClassName: SelectExecution
 * @Date: 13 March, 2021
 * @Author: Cyan
 */
public class QueryInterpreter {
    private PlainSelect plainSelect;

    // basic elements in plain select
    private Distinct distinct;
    private List<SelectItem> selectItems;
    private FromItem fromItem;
    private List<Join> joins;
    private Expression where;
    private List<OrderByElement> orderByElements;

    // and expressions extracted from where
    private List<Expression> andExpressions;

    // tables appeared in query
    private List<String> tables;

    // conditions types: constant, select, join
    private List<Expression> constantCondition;
    private Map<String, List<Expression>> selectCondition;
    private Map<String, List<Expression>> joinCondition;

    // table and corresponding contacted conditions
    // mainly used in tree building
    private Map<String, Expression> selectConditionCombination;
    private Map<String, Expression> joinConditionCombination;

    // tree root
    private Operator root;

    public QueryInterpreter(Statement statement) {
        this.plainSelect = (PlainSelect) ((Select) statement).getSelectBody();

        // init necessary conditions
        this.distinct = plainSelect.getDistinct(); // distinct
        this.selectItems = plainSelect.getSelectItems(); // select
        this.fromItem = plainSelect.getFromItem(); // from
        this.joins = plainSelect.getJoins(); // from
        this.where = plainSelect.getWhere(); // where
        this.orderByElements = plainSelect.getOrderByElements(); // order by

        // init and expressions
        this.andExpressions = processWhereToExpressions(where);

        // init collections
        this.tables = new ArrayList<>();
        this.constantCondition = new ArrayList<>();
        this.selectCondition = new LinkedHashMap<>();
        this.joinCondition = new LinkedHashMap<>();
        this.selectConditionCombination = new LinkedHashMap<>();
        this.joinConditionCombination = new LinkedHashMap<>();

        // process from, generate tables, generate select and join condition keys
        DBCatalog.getInstance().getAliasToTable().clear();
        processFromItem(fromItem);

        if (joins != null) {
            for (Join join : joins) {
                FromItem joinFromItem = join.getRightItem();
                processFromItem(joinFromItem);
            }
        }

        // process and expressions to constant, select or join conditions
        for (Expression ex : andExpressions) {
            List<String> tableNames = getTableNamesInExpression(ex);

            if (tableNames.size() == 0) { // no tables means it is a constant expression
                constantCondition.add(ex);
                continue;
            }

            int index = getTableNamesMaxIndex(tableNames);
            if (tableNames.size() == 1) { // one table means it is a select expression
                selectCondition.get(tables.get(index)).add(ex);
            } else { // two tables means it is a join expression
                joinCondition.get(tables.get(index)).add(ex);
            }
        }

        // combine selection and join expressions to one for each table
        for (String table : tables) {
            selectConditionCombination.put(table, processExpressionCombination(selectCondition.get(table)));
            joinConditionCombination.put(table, processExpressionCombination(joinCondition.get(table)));
        }

        // execute
        executeQueryPlan();
    }

    public void executeQueryPlan() {
        List<String> singleSchema = DBCatalog.getInstance().generateTableSchema(tables.get(0));
        BufferedReader bufferedReader = DBCatalog.getInstance().generateTableBuffer(tables.get(0));
        root = new ScanOperator(tables.get(0), singleSchema, bufferedReader);

        // base table
        if (selectConditionCombination.get(tables.get(0)) != null) {
            root = new SelectOperator(selectConditionCombination.get(tables.get(0)), root);
        }

        // if more than one table, then do select and join
        if (tables.size() > 1) {
            int i = 1;
            while (i < tables.size()) {
                List<String> otherSchema = DBCatalog.getInstance().generateTableSchema(tables.get(i));
                BufferedReader otherReader = DBCatalog.getInstance().generateTableBuffer(tables.get(i));
                Operator otherOperator = new ScanOperator(tables.get(i), otherSchema, otherReader);

                if (selectConditionCombination.get(tables.get(i)) != null) {
                    otherOperator = new SelectOperator(selectConditionCombination.get(tables.get(i)), otherOperator);
                }

                // Ensure left-associativity in join, we add left child as the
                // current table .
                // Then we add the right join with a scan parent (as tables are
                // always leaves and have a scan immediately above them)
                root = new JoinOperator(joinConditionCombination.get(tables.get(i)), root, otherOperator);

                i++;
            }
        }



        if (orderByElements != null) { // need sorting
            if (!(selectItems.get(0) instanceof AllColumns)) { // sorting plus project

                // sort and project -> extract as a method
                boolean sortBeforeProject;

                Set<String> selectedColumns = new HashSet<>();
                Set<String> orderedColumns = new HashSet<>();

                for (OrderByElement orderByElement : orderByElements)
                    orderedColumns.add(orderByElement.toString());

                for (SelectItem selectItem : selectItems)
                    selectedColumns.add(selectItem.toString());

                orderedColumns.removeAll(selectedColumns);
                sortBeforeProject = !orderedColumns.isEmpty();



                if (sortBeforeProject) {
                    root = new ProjectOperator(selectItems, new SortOperator(orderByElements, root));
                } else {
                    root = new SortOperator(orderByElements, new ProjectOperator(selectItems, root));
                }

            } else { // only sorting
                root = new SortOperator(orderByElements, root);
            }
        } else {// no order-by so just select all columns // only projecting
            // projection
            if (!(selectItems.get(0) instanceof AllColumns)) { // only project when not all columns required
                root = new ProjectOperator(selectItems, root);
            }
        }


        // last step, distinct



    }

    public void output(PrintStream ps) {
        root.dump(ps);
    }

    private Expression processExpressionCombination(List<Expression> expressions) {
        if (expressions.size() == 0) {
            return null;
        }

        Expression combination = expressions.get(0);

        for (int i = 1; i < expressions.size(); i++) {
            combination = new AndExpression(combination, expressions.get(i));
        }

        return combination;
    }

    private int getTableNamesMaxIndex(List<String> tableNames) {
        int pos = 0;
        for (String tableName : tableNames) {
            //pos = Math.max(tables.indexOf(tableName), pos);
            int temp = tables.indexOf(tableName);
            pos = (temp > pos) ? temp : pos;
        }
        return pos;
    }

    private List<String> getTableNamesInExpression(Expression expression) {
        List<String> tableNames = new ArrayList<>();

        if ((expression instanceof BinaryExpression)) { // only for binary expression
            BinaryExpression binaryExpression = (BinaryExpression) expression;
            Expression left = binaryExpression.getLeftExpression();
            Expression right = binaryExpression.getRightExpression();

            Column column;
            if (left instanceof Column) {
                column = (Column) left;
                tableNames.add(column.getTable().toString());
            }
            if (right instanceof Column) {
                column = (Column) right;
                // add only if we have a different table
                if (!(tableNames.size() == 1 && tableNames.get(0).equals(column.getTable().toString()))) {
                    tableNames.add(column.getTable().toString());
                }
            }
        }
        return tableNames;
    }

    private void processFromItem(FromItem fi) {
        String name;
        if (fi.getAlias() != null) { // if alias exists, set its alias
            name = fi.getAlias().toString().trim();
            DBCatalog.getInstance().getAliasToTable().put(name, fi.toString().split(" ")[0]);
        } else { // else use table name
            name = fi.toString();
        }
        tables.add(name);

        selectCondition.put(name, new ArrayList<>());
        joinCondition.put(name, new ArrayList<>());
    }

    private List<Expression> processWhereToExpressions(Expression expression) {
        List<Expression> expressions = new ArrayList<>();
        while (expression instanceof AndExpression) {
            AndExpression andExpression = (AndExpression) expression;
            expressions.add(0, andExpression.getRightExpression()); // 0 -> correct the order
            expression = andExpression.getLeftExpression();
        }
        expressions.add(expression);

        return expressions;
    }
}
