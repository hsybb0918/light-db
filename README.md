# Lightweight Database Management System

The DBMS translates from select-from-where queries to relational algebra query plans,  and uses the iterator model for relational operator evaluation. The most common operators are implemented, including selection, projection, join, sort and distinct.

NOTE: The explanation of the logic for extracting join conditions from the where clause is mentioned in **Query Interpreter** section.

## Notice of use

+ Make sure the output directory (e.g. ```output/```) is already exist, but you do not have to have the output file (e.g. ```query.csv```) exist.
+ The function ```parseQuery()``` is used to create the ```QueryInterpret``` class and output the result to print stream. You can run the code through ```jar``` processed by maven like in the instruction.

## Construction of code

There are three packages in the code part:

+ <u>models</u>:
  + **Tuple**: include a linked hash map which maps the column name prefixed with the table name or alias to the value, this can make it unnecessary to pass the schema of the tuple every time.
    
+ <u>operators</u>:
  + **Operator**: abstract operator class.
  + **ScanOperator**: *one child*, every table will have a scan operator to read the tuple line by line.
  + **SelectOperator**: *one child*, use select visitor to determine whether the tuple satisfies the select condition.
  + **ProjectionOperator**: *one child*, project on certain columns.  
  + **JoinOperator**: *two children*, use join visitor to determine whether the two tuples satisfy the join condition, and combine the two tuples if they satisfy.
  + **SortOperator**: *one child*, a blocking operator to deal with order by clause, use a custom tuple comparator to compare the tuples.
  + **DuplicateEliminationOperator**: *one child*, use a hash set to remove the duplicated tuples.
    
+ <u>tools</u>:
  + **DBCatalog**: deal with the database directory, store the alias to table name mapping and the table name to schema mapping.
  + **ConstantVisitor**: visitor to determine the constant conditions like 42 = 42, require *no tuple*.  
  + **SelectVisitor**: visitor to determine the select conditions, require *one tuple*.
  + **JoinVisitor**: visitor to determine the join conditions, require *two tuples*. 
  + **TupleComparator**: comparator used in sort operator.
  + **QueryInterpreter**: the most important class, interpret the query and execute the query plan through tree building.
    
## Explanation of logic

The logic of interpreting the query and building the query plan is written in the **QueryInterpreter** class, so this class will be explained in this part.

There are two prime function in the class, that is ```interpretQuery()``` and ```executeQuery()```.

### Query interpreter

The function ```interpretQuery()``` (line 81) is used to extract different parts, including the constant, select and join conditions in the ```statement```, which lays the foundation for the query executor.

#### STEP 1: Extract tables

First, the tables are extract from ```FromItem``` and ```List<Join>```. The order of the tables should be consistent with the order in the query. The mapping on aliases is created if the tables use aliases.

#### STEP 2: Classify conditions

The ```where``` expression is split into single expressions on ```and``` in the constructor, so the next part is to classify the single expressions. The expressions are classified into three categories: **constant** (```42 = 42```, no table), **select** (```S.A = 1```, one table) and **sort** (```S.A > R.B```, two tables) conditions. According to the number of tables in the expression, these expressions can be classified into the three categories.

**Constant condition** is just a list of expressions ```List<Expression>```. **Select condition** is a ```LinkedHashMap<String, List<Expression>>```, which maps the table name or alias to the list of expressions on this table. **Join condition** is the most important part, which is also a ```LinkedHashMap<String, List<Expression>>```. It is known that the table names are stored in order, and the join condition expression must have two tables. The question is to put this join condition expression into the mapping of which table. Here is the answer: the join condition expression is stored in the mapping of the table name with the max index in the table list because of the left deep join. For example, the table list is ```[R, S, T]``` and the join condition expression is ```T.A < R.B```, then the expression is stored in the mapping of the table ```T```, since when you justify the join condition, you must make sure another table exist.

#### STEP 3: Combine conditions

After classifying the three categories of conditions, we need to combine the list of expressions into one ```AndExpression```, in order to use the visitors to evaluate them in the next part.

### Query executor

The function ```executeQuery()``` (line 84) is used to construct the tree and execute the query plan.

#### STEP 1: Scan base table

Since at least one table exists in the table list, the base table is the first table in the table list. The ```ScanOperator``` is first applied on this table.

#### STEP 2: Select and join

Select and join operators should be applied to other tables if there is more than one table in the list. But before applying, the constant condition should be evaluated.

+ If no constant condition, apply select and join operator on tables. Each table should first operate on select condition, then it can be joined by left deep join.
+ If have constant condition, there are two cases:
  + constant condition is **true**, then the constant condition can be ignored, apply select and join operator on tables as normal.
  + constant condition is **false**, the whole ```where``` clause can be ignored, and only join operator is applied on the tables.
    
#### STEP 3: Sort and project

Here the implementation ensures that you can sort before the projection, that is to say, you can sort on the columns that are not projected.

+ If have ```order by```, sort and project operators should be considered:
  + if ```SelectExpressionItem```, sort and project operators are both required, so to first do sorting or projecting should be determined:
    + if sorted columns are in projected columns, <u>first project then sort</u>.
    + if sorted columns are not in projected columns, <u>first sort then project</u>.
  + if ```AllColumns```, <u>only sort</u>.
+ If no ```order by```, just project operator should be considered:
  + if ```SelectExpressionItem```, <u>only projection</u>.
  + if ```AllColumns```, <u>do nothing</u>.

#### STEP 4: Distinct

Since ```HashSet<Tuple>``` is used here to exclude duplicated tuples, we don't have to worry about whether the original tuples are sorted or not. If there is ```distinct```, the operator is directly applied.

#### STEP 5: Set the root

The last step is to set the root. The ```output()``` is used to output to the stream from the root.