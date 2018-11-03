package util;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.schema.Column;

import java.util.ArrayList;
import java.util.List;

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
            if (!(e instanceof EqualsTo)) {
                others.add(e);
                continue;
            }

            EqualsTo et = (EqualsTo) e;
            String left = et.getLeftExpression().toString();
            String right = et.getRightExpression().toString();

            boolean ol = outScm.contains(left),
                    or = outScm.contains(right),
                    il = inScm.contains(left),
                    ir = inScm.contains(right);

            if (ol && or || il && ir ||
                    !(ol && ir || or && il)) {
                others.add(e);
                continue;
            }

            if (or) {
                et = new EqualsTo(et.getRightExpression(),
                        et.getLeftExpression());
                String tmp = left; left = right; right = tmp;
            }

            int outIdx = getAttrIdx(left, outScm);
            int inIdx = getAttrIdx(right, inScm);

            if (outIdx == -1 || inIdx == -1)
                throw new IllegalArgumentException();

            outIdxs.add(outIdx);
            inIdxs.add(inIdx);
            eqs.add(et);
        }

        eqs.addAll(others);
        return genAnds(eqs);
    }

    private static List<Expression> getAndExpressions(Expression where) {
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
    public static boolean withIndexed(String tab, Expression expression) {
        indexInfo ii = DBCatalog.getindexInfo(tab);
        if (expression == null || ii == null) return false;
        List<Expression> conds = getAndExpressions(expression);

        for (Expression expr : conds) {
            Expression left =
                    ((BinaryExpression) expr).getLeftExpression();
            Expression right =
                    ((BinaryExpression) expr).getRightExpression();

            String str = null;

            if (left instanceof Column && right instanceof LongValue
                    || left instanceof LongValue && right instanceof Column) {
                if (   expr instanceof EqualsTo
                        || expr instanceof GreaterThan
                        || expr instanceof GreaterThanEquals
                        || expr instanceof MinorThan
                        || expr instanceof MinorThanEquals ) {

                    str = (left instanceof Column) ? left.toString() :
                            right.toString();
                    if (str.indexOf('.') != -1)
                        str = str.split("\\.")[1];
                    if (ii.indexCol.equals(str)) return true;
                }

            }
            str = (left instanceof Column) ? left.toString() :
                    right.toString();
            if (str.indexOf('.') != -1)
                str = str.split("\\.")[1];

            if (ii.indexCol.equals(str)) return true;
        }

        return false;

    }




}
