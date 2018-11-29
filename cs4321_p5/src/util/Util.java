package util;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static util.Util.getAttrIdx;

public class Util {
    public static Expression procJoinConds(Expression exp,
                                           List<String> outScm, List<String> inScm,
                                           List<Integer> outIdxs, List<Integer> inIdxs) {
            outIdxs.clear(); inIdxs.clear();
            if (exp == null) return null;

            List<Expression> exps = getAndExpressions(exp);
            List<Expression> eqs = new ArrayList<Expression>();
            List<Expression> others = new ArrayList<Expression>();

            for (Expression e : exps) {
//                if (!(e instanceof EqualsTo)) {
//                    others.add(e);
//
//                    continue;
//                }

                BinaryExpression et = (BinaryExpression) e;
                String left = et.getLeftExpression().toString();
                String right = et.getRightExpression().toString();

                boolean ol = outScm.contains(left),
                        or = outScm.contains(right),
                        il = inScm.contains(left),
                        ir = inScm.contains(right);

                if (ol && or || il && ir ||
                        ol && ir || or && il) {
                    eqs.add(e);

                    continue;
                }

//                if (or) {
//                    et = new EqualsTo(et.getRightExpression(),
//                            et.getLeftExpression());
//                    String tmp = left; left = right; right = tmp;
//                }
//
//                int outIdx = getAttrIdx(left, outScm);
//                int inIdx = getAttrIdx(right, inScm);
//
//                if (outIdx == -1 || inIdx == -1)
//                    throw new IllegalArgumentException();
//
//                outIdxs.add(outIdx);
//                inIdxs.add(inIdx);
//                eqs.add(et);
            }

            //eqs.addAll(others);
            return genAnds(eqs);
    }

    public static List<Expression> getAndExpressions(Expression where) {
        List<Expression> temp = new ArrayList<>();
        while(where instanceof AndExpression) {
            AndExpression and = (AndExpression) where;
            temp.add(and.getRightExpression());
            where = and.getLeftExpression();
        }
        temp.add(where);
        return temp;
    }
    public static Expression genAnds(List<Expression> exps) {
        if (exps.isEmpty()) return null;
        Expression ret = exps.get(0);
        for (int i = 1; i < exps.size(); i++)
            ret = new AndExpression(ret, exps.get(i));
        return ret;
    }

    public static int getAttrIdx(String attr, List<String> schema) {
        int idx = schema.indexOf(attr);
        if (idx != -1) return idx;

        for(int i = 0; i < schema.size(); i++) {
            String colName = getColName(schema.get(i));
            if (colName.equals(attr))
                return i;
        }

        return -1;
    }
    public static String getColName(String tabCol) {
        return tabCol.split("\\.")[1];
    }

    public static Integer[] getLowAndHeigh(String idxCol, Expression expression) {
        if (expression == null) return null;
        List<Expression> conds = getAndExpressions(expression);

        Integer[] ret = new Integer[2]; // low and high

        for (Expression expr : conds) {
            Expression left =
                    ((BinaryExpression) expr).getLeftExpression();
            Expression right =
                    ((BinaryExpression) expr).getRightExpression();

            String attr = null;
            Integer val = null;
            if (left instanceof Column) {
                attr = left.toString();
                val = Integer.parseInt(right.toString());
            }
            else {
                attr = right.toString();
                val = Integer.parseInt(left.toString());
            }
            if (attr.indexOf('.') != -1)
                attr = attr.split("\\.")[1];
            if (!idxCol.equals(attr)) continue;
            // TODO
            // update low key and high key
            //update low key
            if(expr instanceof GreaterThan){ // inclusive low key
                if(ret[0] == null){
                    ret[0] = val+1;
                } else{
                    ret[0] = Math.max(ret[0], val+1);
                }

            } else if (expr instanceof GreaterThanEquals) {
                if(ret[0] == null){
                    ret[0] = val;
                } else{
                    ret[0] = Math.max(ret[0], val);
                }
            }else if(expr instanceof MinorThan){
                if(ret[1] == null){
                    ret[1] = val;
                } else {
                    ret[1] = Math.min(ret[1],val);
                }
            } else if(expr instanceof MinorThanEquals){ // exclusive high key
                if(ret[1] == null){
                    ret[1] = val+1;
                } else {
                    ret[1] = Math.min(ret[1], val+1);
                }
            } else if (expr instanceof EqualsTo){
                ret[0] = val;
                ret[1] = val;
            }
        }
        return ret;
    }
//    public static boolean withIndexed(String tab, Expression expression) {
//        indexInfo ii = DBCatalog.getindexInfo(tab);
//        if (expression == null || ii == null) return false;
//        List<Expression> conds = getAndExpressions(expression);
//
//        for (Expression expr : conds) {
//            Expression left =
//                    ((BinaryExpression) expr).getLeftExpression();
//            Expression right =
//                    ((BinaryExpression) expr).getRightExpression();
//
//            String str = null;
//
//            if (left instanceof Column && right instanceof LongValue
//                    || left instanceof LongValue && right instanceof Column) {
//                if (   expr instanceof EqualsTo
//                        || expr instanceof GreaterThan
//                        || expr instanceof GreaterThanEquals
//                        || expr instanceof MinorThan
//                        || expr instanceof MinorThanEquals ) {
//
//                    str = (left instanceof Column) ? left.toString() :
//                            right.toString();
//                    if (str.indexOf('.') != -1)
//                        str = str.split("\\.")[1];
//                    if (ii.indexCol.equals(str)) return true;
//                }
//
//            }
//            str = (left instanceof Column) ? left.toString() :
//                    right.toString();
//            if (str.indexOf('.') != -1)
//                str = str.split("\\.")[1];
//
//            if (ii.indexCol.equals(str)) return true;
//        }
//
//        return false;
//
//    }
    public static String  getFullTableName(String name) {
        name = DBCatalog.alias.containsKey(name) ? DBCatalog.alias.get(name) : name;
        return name;

    }

    private static void updateRange(Integer[] range, int val,
                                    boolean isLower, boolean inclusive, boolean oppo) {
        if (oppo) {
            updateRange(range, val, !isLower, inclusive, false);
            return;
        }

        if (!inclusive)
            val = (isLower) ? val + 1 : val - 1;

        if (isLower)
            range[0] = (range[0] == null) ? val :
                    Math.max(range[0], val);
        else
            range[1] = (range[1] == null) ? val :
                    Math.min(range[1], val);
    }
    /**
     *
     * @param exp
     * @param attr
     * @return int[]rst  rst[0] max, rst[1] min
     */
    public static Integer[] getSelRange(Expression exp, String[] attr) {
        if (!isSelect(exp))
            throw new IllegalArgumentException();

        Expression left =
                ((BinaryExpression) exp).getLeftExpression();
        Expression right =
                ((BinaryExpression) exp).getRightExpression();

        Integer val = null;

        if(!(left instanceof LongValue) && !(right instanceof LongValue)) return null;
        if (left instanceof Column) {
            attr[0] = left.toString();
            val = Integer.parseInt(right.toString());
        }
        else {
            attr[0] = right.toString();
            val = Integer.parseInt(left.toString());
        }

        boolean oppo = !(left instanceof Column);
        boolean inclusive = !(exp instanceof MinorThan) &&
                !(exp instanceof GreaterThan);
        boolean isUpper = (exp instanceof MinorThan ||
                exp instanceof MinorThanEquals ||
                exp instanceof EqualsTo);
        boolean isLower = (exp instanceof GreaterThan ||
                exp instanceof GreaterThanEquals ||
                exp instanceof EqualsTo);

        if (!isLower && !isUpper)
            throw new IllegalArgumentException();

        Integer[] ret = new Integer[2];

        if (isLower)
            updateRange(ret, val, true, inclusive, oppo);
        if (isUpper)
            updateRange(ret, val, false, inclusive, oppo);

        return ret;
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
    public static boolean isSelect(Expression exp) {
        List<String> tmp = getExpTabs(exp);
        return (tmp != null && tmp.size() == 1);
    }

    public static boolean isJoin(Expression exp) {
        List<String> tmp = getExpTabs(exp);
        return (tmp != null && tmp.size() == 2);
    }
    public static List<String> getExpTabs(Expression exp) {
        List<String> ret = new ArrayList<String>();
        if (!(exp instanceof BinaryExpression))
            return ret;

        BinaryExpression be = (BinaryExpression) exp;
        Expression left = be.getLeftExpression();
        Expression right = be.getRightExpression();

        Column col;
        if (left instanceof Column) {
            col = (Column) left;
            if (col.getTable() == null) return null;
            ret.add(col.getTable().toString());
        }
        if (right instanceof Column) {
            col = (Column) right;
            if (col.getTable() == null) return null;
            ret.add(col.getTable().toString());
        }

        if (ret.size() == 2 && ret.get(0).equals(ret.get(1)))
            ret.remove(1);

        return ret;
    }

    public static class compareTable implements Comparator<String> {

        @Override
        public int compare(String t1, String t2) {
            t1 = Util.getFullTableName(t1);
            t2 = Util.getFullTableName(t2);
            int sz1 = DBCatalog.tablestats.get(t1).totalTuple;
            int sz2 =  DBCatalog.tablestats.get(t2).totalTuple;
            return Integer.compare(sz1, sz2);
        }

    }

    public static Expression createCondition(String tab, String col,
                                             int val, boolean isEq, boolean isGE) {
        Table t = new Table(null, tab);
        Column c = new Column(t, col);
        LongValue v = new LongValue(String.valueOf(val));

        if (isEq)
            return new EqualsTo(c, v);
        if (isGE)
            return new GreaterThanEquals(c, v);
        return new MinorThanEquals(c, v);
    }
    public static boolean checkEqual(Expression expression){
        List<Expression> exps = getAndExpressions(expression);
        for (Expression e : exps) {
            if (!(e instanceof EqualsTo)) {
                return true;
            }
        }
        return false;
    }




}
