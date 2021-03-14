package ed.inf.adbs.lightdb.tools;

import ed.inf.adbs.lightdb.operators.Operator;
import ed.inf.adbs.lightdb.operators.ProjectOperator;
import ed.inf.adbs.lightdb.operators.ScanOperator;
import ed.inf.adbs.lightdb.operators.SelectOperator;
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
        BufferedReader br = DBCatalog.getInstance().generateTableBuffer(tables.get(0));
        root = new ScanOperator(tables.get(0), singleSchema, br);

        if (selectConditionCombination.get(tables.get(0)) != null) {
            root = new SelectOperator(selectConditionCombination.get(tables.get(0)), root);
        }

        if (!(selectItems.get(0) instanceof AllColumns)) {
            root = new ProjectOperator(selectItems, root);
        }

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
