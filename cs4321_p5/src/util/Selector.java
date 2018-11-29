package util;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.invoke.StringConcatFactory;
import java.util.*;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
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
import operators.*;
import unionfind.UnionFind;
import unionfind.UnionFindElement;
import util.DBCatalog;
import util.Table;
import visitors.PhysicalPlanBuilder;

import static visitors.PhysicalPlanBuilder.optimalJoin;


/**
 * Selector parses SQL queries and builds the SQL operator tree
 *
 * @authors Yao Xiao, Kyle Johnson, Valerie Tan
 */
public class Selector {

    public Operator root = null;
    public LogicalOperator logicalRoot = null;
    public FromItem from;
    public List<SelectItem> selects;
    public PlainSelect plainsel;
    public Select sel;
    public Distinct distinct;

    public List<Join> joins;
    public Expression where;
    public List<OrderByElement> sort;

    public List<String> fromItems;
    // exps storing every expression with AND
    public List<Expression> exps;

    //selectcondition key is table name, value is the expression
    //associated with the table name
    // "WHERE table1.A = table2.B"
    // table 1: table1.A = table2.B
    // table 2: table1.A = table2.B
    HashMap<String,List<Expression>> selectPlan;
    HashMap<String,List<Expression>> joinPlan;
    HashMap<String, List<Expression>> oldJoinPlan = new HashMap<>();



    public static HashMap<String,Expression> selectCondition = new HashMap<>();
    public static HashMap<String,Expression> joinCondition = new HashMap<>();
    public static HashMap<String,Expression> oldJoinCons = new HashMap<>();


    public static UnionFind unionFind = new UnionFind();
    private boolean selfJoin = false;


    /**
     * constructs a Selector
     *
     * @param statement the SQL query to be parsed by Selector
     */
    public Selector(Statement statement) throws IOException{
        sel = (Select) statement;
        PlainSelect ps = (PlainSelect) sel.getSelectBody();
        fromItems = new ArrayList<>();
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
        //put bigger table to inner relation
        Collections.sort(fromItems, new Util.compareTable());


        for (String table : fromItems) {
            selectPlan.put(table, new ArrayList<>());
            joinPlan.put(table, new ArrayList<>());
            oldJoinPlan.put(table, new ArrayList<>());
        }

        List<Expression> andExpressions = getAndExpressions(where);
   //     construct selection and Projection based on FromItems sequence
        boolean[] marked = new boolean[fromItems.size()];
        if(where != null) {
            for (Expression exp : andExpressions) {
                List<String> tables = tableInAnds(exp);

                int idx = rightMost(tables);
                if (tables == null) {
                    joinPlan.get(fromItems.get(fromItems.size() - 1)).add(exp);
                    oldJoinPlan.get(fromItems.get(fromItems.size() - 1)).add(exp);
                    return;
                }
                switch (tables.size()) {
                    case 0:
                        marked[idx] = true;
                        selectPlan.get(fromItems.get(idx)).add(exp);
                        break;
                    case 1:
                        String[] col = new String[1];
                        Integer[] range = Util.getSelRange(exp, col);
                        if(range == null && !(exp instanceof EqualsTo)) {
                            selectPlan.get(fromItems.get(idx)).add(exp);
                            break;
                        }
                        BinaryExpression bi = (BinaryExpression) exp;
                        Column cols  =(Column) ((bi.getLeftExpression() instanceof LongValue) ?
                                bi.getRightExpression() : bi.getLeftExpression());
                        UnionFindElement ufe = unionFind.find(cols);


                        if (range[0] != null && range[0].equals(range[1])) {
                            ufe.setEqual(range[0]);

                        }

                        else {
                            if (range[0] != null) {
                                ufe.setLow(range[0]);

                            }
                            if (range[1] != null) {
                                ufe.setHigh(range[1]);
                            }

                        }
                        break;
                    case 2:


                        int idx1 = fromItems.indexOf(tables.get(0));
                        int idx2 = fromItems.indexOf(tables.get(1));
                        if(!marked[idx1]) {
                            marked[idx1] = true;
                            String aa = fromItems.get(idx1);
                            oldJoinPlan.get(fromItems.get(idx1)).add(exp);
                        } else if(!marked[idx2]){
                            marked[idx2] = true;
                            String aa = fromItems.get(idx2);
                            oldJoinPlan.get(fromItems.get(idx2)).add(exp);
                        } else {
                            marked[idx] = true;

                            String aa = fromItems.get(idx);
                            oldJoinPlan.get(fromItems.get(idx)).add(exp);
                        }
                        if (exp instanceof EqualsTo) {
                            BinaryExpression be = (BinaryExpression) exp;
                            Column left = (Column) be.getLeftExpression();

                            Column right = (Column) be.getRightExpression();

                            UnionFindElement leftE = unionFind.find(left);
                            UnionFindElement rightE = unionFind.find(right);
                            unionFind.union(leftE, rightE);

                        }
                        else {
                            joinPlan.get(fromItems.get(idx)).add(exp);
                           // oldJoinPlan.get(fromItems.get(idx)).add(exp);
                        }
                        break;


                }

            }
        }

        // have to manually add tables that reside in union find
        for (UnionFindElement uniEle: unionFind.getUnions()) {
            for(Column column : uniEle.getColumns()) {

                String tab = column.getWholeColumnName().split("\\.")[0];
                String col = column.getColumnName();
                List<Expression> lst = selectPlan.get(tab);
                if(lst == null) {
                    lst = new ArrayList<>();
                }
                Integer eq = uniEle.getEqual();
                Integer lower = uniEle.getLow();
                Integer upper = uniEle.getHigh();
                if (eq != null) {
                    lst.add(Util.createCondition(
                            tab, col, eq, true, false));
                }
                else {
                    if (lower != Integer.MIN_VALUE && lower != null)
                        lst.add(Util.createCondition(
                                tab, col, lower, false, true));
                    if (upper != Integer.MAX_VALUE && upper != null)
                        lst.add(Util.createCondition(
                                tab, col, upper, false, false));
                }

            }

        }
        joinCondition = new HashMap<String, Expression>();
        oldJoinCons = new HashMap<String, Expression>();
        selectCondition = new HashMap<String, Expression>();
//        for (String tab : fromItems) {
//            joinCondition.put(tab, Util.genAnds(joinPlan.get(tab)));
//            oldJoinCons.put(tab, Util.genAnds(oldJoinPlan.get(tab)));
//            selectCondition.put(tab, Util.genAnds(selectPlan.get(tab)));
//        }
        for(String table : fromItems) {
            List<Expression> exps1 = joinPlan.get(table);
            List<Expression> expsold = oldJoinPlan.get(table);
            if(!exps1.isEmpty()) {
            Expression res1 = exps1.get(0);
                for (int i = 1; i < exps1.size(); i++) {
                    res1 = new AndExpression(res1, exps1.get(i));

                }
                joinCondition.put(table, res1);
            }
            if(!expsold.isEmpty()) {
                Expression res1 = expsold.get(0);
                String exp = res1.toString();
                for (int i = 1; i < expsold.size(); i++) {
                    res1 = new AndExpression(res1, expsold.get(i));

                }
                oldJoinCons.put(table, res1);
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

       // buildLogicTree();


        buildTree();










    }

    /**
     *
     * @param idx
     * @return get select condition from queries
     */
    public  Expression getSelectCondition(int idx) {

        return selectCondition.get(fromItems.get(idx));
    }
    private Expression getOldJoinCond(int idx) {
        String ss = fromItems.get(idx);
        return oldJoinCons.get(fromItems.get(idx));
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
            idx = Math.max(idx, fromItems.indexOf(table));
        }
        return idx;
    }

    /**
     * Build a logical tree based on cost estimation
     */
    public void buildLogicTree() {
        List<LogicalOperator> res = new ArrayList<>();
        LogicalOperator op;
        String state = fromItems.toString();
        for (int i = 0; i < fromItems.size(); ++i) {
            LogicalOperator table = new LogicalScan(getTable(i));
            if (getSelectCondition(i) != null) {
                table = new LogicalSelect(getSelectCondition(i), table);

            }
            res.add(table);
        }
        if (fromItems.size() > 1) {
            op = new LogicalMassJoin(fromItems, res, oldJoinCons, unionFind);
        } else {
            op = res.get(0);
        }
        if (selects != null) {
            op= new LogicalProject(selects, op);
        }
        if (sort != null) {
            op= new LogicalSort(sort, op);
        }
        if (distinct != null) {
            if (sort == null) {
                op = new LogicalSort(new ArrayList<>(), op);
            }
            op = new LogicalEliminator(op);
        }
        logicalRoot = op;
       // PhysicalPlanBuilder planBuilder = new PhysicalPlanBuilder();
       // logicalRoot.accept(planBuilder);






    }

    /**
     * builds the Physical operator tree based on the query
     */
    public void buildTree()  {
//        LogicalOperator curRoot = new LogicalScan(this.getTable(0));
//        if (getSelectCondition(0) != null) {
//            curRoot = new LogicalSelect(getSelectCondition(0), curRoot);
//        }
//        String ccc = fromItems.toString();
//        for (int i = 1; i < fromItems.size(); ++i) {
//            LogicalOperator op = new LogicalScan(getTable(i));
//            if (getSelectCondition(i) != null) {
//                op = new LogicalSelect(getSelectCondition(i), op);
//            }
//            String con = getOldJoinCond(i).toString();
//                curRoot = new LogicalJoin(getOldJoinCond(i), curRoot, op);
//
//        }
//            if (selects != null) {
//                curRoot = new LogicalProject(selects, curRoot);
//            }
//            if (sort != null) {
//                curRoot = new LogicalSort(sort,  curRoot);
//            }
//            if (distinct != null) {
//                if (sort == null) {
//                    curRoot = new LogicalSort(new ArrayList<>(), curRoot);
//                }
//                curRoot = new LogicalEliminator( curRoot);
//            }
//
        List<LogicalOperator> res = new ArrayList<>();
        LogicalOperator op;
        String state = fromItems.toString();
        for (int i = 0; i < fromItems.size(); ++i) {
            LogicalOperator table = new LogicalScan(getTable(i));
            if (getSelectCondition(i) != null) {
                table = new LogicalSelect(getSelectCondition(i), table);

            }
            res.add(table);
        }
        if (fromItems.size() > 1) {
            op = new LogicalMassJoin(fromItems, res, oldJoinCons, unionFind);
        } else {
            op = res.get(0);
        }
        if (selects != null) {
            op= new LogicalProject(selects, op);
        }
        if (sort != null) {
            op= new LogicalSort(sort, op);
        }
        if (distinct != null) {
            if (sort == null) {
                op = new LogicalSort(new ArrayList<>(), op);
            }
            op = new LogicalEliminator(op);
        }

        PhysicalPlanBuilder planBuilder = new PhysicalPlanBuilder();
        op.accept(planBuilder);
        root = planBuilder.getRoot();




    }

    /**
     *
     * @param idx
     * @return get Table from DBCatalog based sequence in fromItems
     */
    public Table getTable(int idx) {
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

                    if(col.getTable().getName().equals(res.get(0))) {
                        selfJoin = true;
                        return res;
                    }
                    res.add(col.getTable().getName());
                }

        }
        return res;
    }

}
