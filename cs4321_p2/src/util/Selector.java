package util;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.invoke.StringConcatFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectItem;
import operators.LogicalEliminator;
import operators.LogicalJoin;
import operators.LogicalOperator;
import operators.LogicalProject;
import operators.LogicalScan;
import operators.LogicalSelect;
import operators.LogicalSort;
import operators.Operator;
import util.DBCatalog;
import util.Table;
import visitors.PhysicalPlanBuilder;


/**
 * Selector parses SQL queries and builds the SQL operator tree
 *
 * @authors Yao Xiao, Kyle Johnson, Valerie Tan
 */
public class Selector {

    public Operator root = null;
    public FromItem from;
    public List<SelectItem> selects;
    public PlainSelect plainsel;
    public Select sel;
    public Distinct distinct;

    public List<Join> joins;
    public Expression where;
    public List<OrderByElement> sort;

    public List<String> fromItems = new ArrayList<>();
    // exps storing every expression with AND
    public List<Expression> exps;
    //selectcondition key is table name, value is the expression
    //associated with the table name
    // "WHERE table1.A = table2.B"
    // table 1: table1.A = table2.B
    // table 2: table1.A = table2.B
    HashMap<String,List<Expression>> selectPlan;
    HashMap<String,List<Expression>> joinPlan;

    public HashMap<String,Expression> selectCondition = new HashMap<>();
    public HashMap<String,Expression> joinCondition = new HashMap<>();


    /**
     * constructs a Selector
     *
     * @param statement the SQL query to be parsed by Selector
     */
    public Selector(Statement statement) throws IOException{
        sel = (Select) statement;
        PlainSelect ps = (PlainSelect) sel.getSelectBody();

        from = ps.getFromItem();
        joins = ps.getJoins();
        selects = ps.getSelectItems();
        sort = ps.getOrderByElements();


        where = ps.getWhere();
        distinct = ps.getDistinct();

        exps = getAndExpressions(where);
         selectPlan = new HashMap<>();
         joinPlan = new HashMap<>();


        //Construct a List of tables mentioned in FROM
        //SELECT * FROM Sailors S, Reserve R
        //getFromItem returns Sailors S
        //joins return Sailors, Reserve
        // and we are getting Reserve R at joins stage
        if (from.getAlias() != null) {

            DBCatalog.alias.put(from.getAlias(), from.toString().split(" ")[0]);
            fromItems.add(from.getAlias());

        } else {
            fromItems.add(from.toString());
        }
        if(joins != null) {

            for(Join j : joins) {

                FromItem item = j.getRightItem();

                if (item.getAlias() != null) {
                    DBCatalog.alias.put(item.getAlias(), item.toString().split(" ")[0]);
                    fromItems.add(item.getAlias());
                } else {
                    fromItems.add(item.toString());
                }
            }

        }

        for (String table : fromItems) {
            selectPlan.put(table, new ArrayList<>());
            joinPlan.put(table, new ArrayList<>());
        }

        List<Expression> andExpressions = getAndExpressions(where);
        //construct selection and Projection based on FromItems sequence
        if(where != null) {
            for (Expression exp : andExpressions) {
                List<String> tables = tableInAnds(exp);
                int idx = rightMost(tables);
                if (tables == null)
                    joinPlan.get(fromItems.get(fromItems.size() - 1)).add(exp);
                else if (tables.size() <= 1)
                    selectPlan.get(fromItems.get(idx)).add(exp);
                else
                    joinPlan.get(fromItems.get(idx)).add(exp);
            }
        }
        for(String table : fromItems) {
            List<Expression> exps1 = joinPlan.get(table);
            if(!exps1.isEmpty()) {
            Expression res1 = exps1.get(0);

                for (int i = 1; i < exps1.size(); i++) {
                    res1 = new AndExpression(res1, exps1.get(i));
                }
                joinCondition.put(table, res1);
            }

            List<Expression> exps2 = selectPlan.get(table);
            if(!exps2.isEmpty()) {
                Expression res2 = exps2.get(0);

                for (int i = 1; i < exps2.size(); i++) {
                    res2 = new AndExpression(res2, exps2.get(i));
                }
                selectCondition.put(table, res2);
            }

        }


        buildTree();







    }

    /**
     *
     * @param idx
     * @return get select condition from queries
     */
    private Expression getSelectCondition(int idx) {

        return selectCondition.get(fromItems.get(idx));
    }

    /**
     *
     * @param idx
     * @return get join condition from queries
     */
    private Expression getJoinCondition(int idx) {
        return joinCondition.get(fromItems.get(idx));
    }

    /**
     * Find the right most table index in FromItems sequence
     * @param tables list
     * @return idx
     */
    private int rightMost(List<String> tables) {

        if (tables == null) return fromItems.size() - 1;
        int idx = 0;
        for (String table : tables) {
            idx = Math.min(idx, fromItems.indexOf(table));
        }
        return idx;
    }
    /**
     * builds the operator tree based on the query
     */
    public void buildTree() throws IOException {
        LogicalOperator curRoot = new LogicalScan(this.getTable(0));
        if (getSelectCondition(0) != null) {
            curRoot = new LogicalSelect(this.getSelectCondition(0), curRoot);
        }
        for (int i = 1; i < this.fromItems.size(); ++i) {
            LogicalOperator op = new LogicalScan(this.getTable(i));
            if (getSelectCondition(i) != null) {
                op = new LogicalSelect(this.getSelectCondition(i), op);
                curRoot = new LogicalJoin(this.getJoinCondition(i),  curRoot, op);
            }
            if (selects != null) {
                curRoot = new LogicalProject(selects, curRoot);
            }
            if (sort != null) {
                curRoot = new LogicalSort(sort,  curRoot);
            }
            if (distinct != null) {
                if (sort == null) {
                    curRoot = new LogicalSort(sort, curRoot);
                }
                curRoot = new LogicalEliminator( curRoot);
            }


        }
        PhysicalPlanBuilder planBuilder = new PhysicalPlanBuilder(DBCatalog.config);
        curRoot.accept(planBuilder);
        this.root = planBuilder.getRoot();
    }

    /**
     *
     * @param idx
     * @return get Table from DBCatalog based sequence in fromItems
     */
    private Table getTable(int idx) {
        return DBCatalog.getTable(fromItems.get(idx));
    }

    /**
     *
     * @param where, decompose consecutive AND conditions
     * @return A list of single AND expression
     */
    private List<Expression> getAndExpressions(Expression where) {
        List<Expression> temp = new ArrayList<>();
        while(where instanceof AndExpression) {
            AndExpression and = (AndExpression) where;
            temp.add(and.getRightExpression());
            where = and.getLeftExpression();
        }
        temp.add(where);
        return temp;
    }

    /**
     *
     * @param exp expression containing tables
     * @return a list of tables appeared in this AND
     */
    private List<String> tableInAnds(Expression exp) {
        List<String> res = new ArrayList<>();
        Expression left = ((BinaryExpression) exp).getLeftExpression();
        Expression right = ((BinaryExpression) exp).getRightExpression();
        if(left instanceof Column) {
            Column col = (Column)left;
            if(col.getTable() != null) res.add(col.getTable().getName());
        }
        if(right instanceof Column) {
            Column col = (Column)right;
            if(col.getTable() != null)
                if(!res.isEmpty()) {

                    if(col.getTable().getName().equals(res.get(0))) return res;
                    res.add(col.getTable().getName());
                }

        }
        return res;
    }

}
