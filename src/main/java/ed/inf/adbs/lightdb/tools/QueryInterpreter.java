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
 * Interpret the query.
 *
 * ClassName: SelectExecution
 * Date: 13 March, 2021
 * Author: Cyan
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

    // tables mapping to the constant, select and join condition
    private List<Expression> constantCondition;
    private Map<String, List<Expression>> selectCondition;
    private Map<String, List<Expression>> joinCondition;

    // tables mapping to the corresponding contacted conditions from above
    // used in tree building as operator parameter
    private Expression constantConditionCombination;
    private Map<String, Expression> selectConditionCombination;
    private Map<String, Expression> joinConditionCombination;

    // tree root
    private Operator root;

    /**
     * Constructor: once call, the interpretation finishes.
     *
     * @param statement query statement
     */
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

        // first interpret query
        interpretQuery();

        // then build the tree, construct the query plan
        executeQuery();
    }

    /**
     * Output to stream.
     *
     * @param ps output stream
     */
    public void output(PrintStream ps) {
        root.dump(ps); // dump from root
    }

    /**
     * Build the query tree.
     */
    private void executeQuery() {
        Operator current;

        // STEP 1: init base table
        // first apply scan operator on the first table
        List<String> singleSchema = DBCatalog.getInstance().generateSingleSchema(tables.get(0));
        BufferedReader bufferedReader = DBCatalog.getInstance().generateTableBuffer(tables.get(0));
        current = new ScanOperator(tables.get(0), singleSchema, bufferedReader);

        // STEP 2: deal with constant condition, apply select and join operator
        // evaluate constant condition
        if (constantConditionCombination != null) {
            ConstantVisitor constantVisitor = new ConstantVisitor();
            constantConditionCombination.accept(constantVisitor);

            // if false, ignore the where clause, means no need to use select operator
            // if true, ignore the constant condition
            if (!constantVisitor.getTupleEvaluationResult()) { // false
                current = applyJoinOperator(current);
            } else { // true
                current = applySelectJoinOperator(current);
            }
        } else { // if no constant condition, do as normal
            current = applySelectJoinOperator(current);
        }

        // STEP 3: deal with sort and project operator
        // if select all columns, no need to project, directly sort
        // if no need to sort, directly project
        // if need to sort and project, then determine on two strategy:
        // --- if sorted columns are in projected columns, first project then sort
        // --- if sorted columns are not in projected columns, first sort then project
        if (orderByElements != null) { // need sort and project
            if (!(selectItems.get(0) instanceof AllColumns)) { // sort plus project

                // whether need to sort then project
                boolean sortBeforeProject;

                Set<String> selectedColumns = new HashSet<>();
                Set<String> orderedColumns = new HashSet<>();

                for (OrderByElement orderByElement : orderByElements)
                    orderedColumns.add(orderByElement.toString());

                for (SelectItem selectItem : selectItems)
                    selectedColumns.add(selectItem.toString());

                orderedColumns.removeAll(selectedColumns);
                sortBeforeProject = !orderedColumns.isEmpty();

                // determine between two strategies
                if (sortBeforeProject) {
                    current = new ProjectOperator(selectItems, new SortOperator(orderByElements, current));
                } else {
                    current = new SortOperator(orderByElements, new ProjectOperator(selectItems, current));
                }

            } else { // no need project, only sort
                current = new SortOperator(orderByElements, current);
            }
        } else { // no need sort, only project
            if (!(selectItems.get(0) instanceof AllColumns)) { // only project when not all columns required
                current = new ProjectOperator(selectItems, current);
            }
        }

        // STEP 4: deal with distinct operator
        if (distinct != null) {
            current = new DuplicateEliminationOperator(current);
        }

        // STEP 5: set the root
        root = current;
    }

    /**
     * Interpret the query.
     */
    private void interpretQuery() {
        // clear alias table
        DBCatalog.getInstance().getAliasToTable().clear();

        // generate tables, init select and join condition in from
        processFromItem(fromItem);

        // generate tables, init select and join condition in join
        if (joins != null) {
            for (Join join : joins) {
                FromItem joinFromItem = join.getRightItem();
                processFromItem(joinFromItem);
            }
        }

        // process and expressions to constant, select or join conditions according to the table
        for (Expression ex : andExpressions) {
            List<String> tableNames = getTableNamesInExpression(ex);

            if (tableNames.isEmpty()) { // no tables means it is a constant expression
                constantCondition.add(ex);
                continue;
            }

            int index = getTableNamesMaxIndex(tableNames); // get max table index
            if (tableNames.size() == 1) { // one table means it is a select expression
                selectCondition.get(tables.get(index)).add(ex);
            } else { // two tables means it is a join expression
                joinCondition.get(tables.get(index)).add(ex);
            }
        }

        // combine constant expressions
        this.constantConditionCombination = processExpressionCombination(constantCondition);

        // combine selection and join expressions to one for each table
        for (String table : tables) {
            selectConditionCombination.put(table, processExpressionCombination(selectCondition.get(table)));
            joinConditionCombination.put(table, processExpressionCombination(joinCondition.get(table)));
        }
    }

    /**
     * Apply select and join operator on all tables.
     *
     * @param current current root
     * @return new root
     */
    private Operator applySelectJoinOperator(Operator current) {
        // if have select condition on the first table, then apply select operator
        if (selectConditionCombination.get(tables.get(0)) != null) {
            current = new SelectOperator(selectConditionCombination.get(tables.get(0)), current);
        }

        // if more than one table, then do select and join operator on tables one by one
        if (tables.size() > 1) {
            int i = 1;
            while (i < tables.size()) {
                // first init scan operator on current table
                List<String> otherSchema = DBCatalog.getInstance().generateSingleSchema(tables.get(i));
                BufferedReader otherReader = DBCatalog.getInstance().generateTableBuffer(tables.get(i));
                Operator node = new ScanOperator(tables.get(i), otherSchema, otherReader);

                // if the table has select condition, apply select operator
                if (selectConditionCombination.get(tables.get(i)) != null) {
                    node = new SelectOperator(selectConditionCombination.get(tables.get(i)), node);
                }

                // since should be a left deep join tree, add the last table as the left child and
                // set current table as the right child
                current = new JoinOperator(joinConditionCombination.get(tables.get(i)), current, node);

                i++; // advance index if more table
            }
        }
        return current;
    }

    /**
     * Apply only join operator on all tables.
     *
     * @param current current root
     * @return new root
     */
    private Operator applyJoinOperator(Operator current) {
        // if more than one table, then join tables one by one
        if (tables.size() > 1) {
            int i = 1;
            while (i < tables.size()) {
                // first init scan operator on current table
                List<String> otherSchema = DBCatalog.getInstance().generateSingleSchema(tables.get(i));
                BufferedReader otherReader = DBCatalog.getInstance().generateTableBuffer(tables.get(i));
                Operator node = new ScanOperator(tables.get(i), otherSchema, otherReader);

                // since should be a left deep join tree, add the last table as the left child and
                // set current table as the right child
                current = new JoinOperator(joinConditionCombination.get(tables.get(i)), current, node);

                i++; // advance index if more table
            }
        }
        return current;
    }

    /**
     * Combine expressions to one and expression.
     *
     * @param expressions list of expressions
     * @return one and expression
     */
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

    /**
     * Get the max table index, it is mainly useful for join condition,
     * the join condition need to process as late as possible, otherwise the table may noy start join.
     *
     * @param tableNames list of table names in expression
     * @return index
     */
    private int getTableNamesMaxIndex(List<String> tableNames) {
        int pos = 0;
        for (String tableName : tableNames) {
            pos = (tables.indexOf(tableName) > pos) ? tables.indexOf(tableName) : pos;
        }
        return pos;
    }

    /**
     * Get the names in the single expression.
     *
     * @param expression expression
     * @return list of table names or alias
     */
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

    /**
     * Process the tables in from clause, and init select and join condition.
     *
     * @param fi from item
     */
    private void processFromItem(FromItem fi) {
        String name;
        if (fi.getAlias() != null) { // if alias exists, set its alias in database catalog
            name = fi.getAlias().toString().trim();
            DBCatalog.getInstance().getAliasToTable().put(name, fi.toString().split(" ")[0]);
        } else { // else use table name, no need to set database catalog
            name = fi.toString();
        }

        // add table name or alias, in order
        tables.add(name);

        // init select and join condition
        selectCondition.put(name, new ArrayList<>());
        joinCondition.put(name, new ArrayList<>());
    }

    /**
     * Split the where clause to expressions
     *
     * @param expression expression
     * @return list of expressions
     */
    private List<Expression> processWhereToExpressions(Expression expression) {
        List<Expression> expressions = new ArrayList<>();
        while (expression instanceof AndExpression) {
            AndExpression andExpression = (AndExpression) expression;
            expressions.add(0, andExpression.getRightExpression()); // 0 -> correct the order
            expression = andExpression.getLeftExpression(); // if and expression, continue to split
        }
        expressions.add(expression);

        return expressions;
    }
}