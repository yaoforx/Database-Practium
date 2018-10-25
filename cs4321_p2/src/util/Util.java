package util;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;

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

            System.out.println("Equals: " + left + ' ' + right);

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

    /**
     * Obtain the tuple's attribute.
     * @param tp the tuple
     * @param attr the attribute
     * @param schema the tuple's schema
     * @return the long value of the attribute
     */
    public static Long getAttrVal(Tuple tp, String attr, List<String> schema) {
        int idx = getAttrIdx(attr, schema);
        if (idx != -1) return (long) tp.getValue(idx);
        return null;
    }



}
