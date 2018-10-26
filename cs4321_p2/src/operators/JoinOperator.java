package operators;

import util.*;
import visitors.JoinVisitors;


import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * JoinOperator contains both a left and right child Operator along with an Expression which represents the
 * join condition
 *
 * @authors Yao Xiao, Kyle Johnson, Valerie Tan
 */
public abstract class JoinOperator extends Operator {
    Expression exp;
    public Operator left;
    public Operator right;
    JoinVisitors jv;
    Tuple l;
    Tuple r;

    /**
     * calls reset() on both the left and right child Operators
     */
    @Override
    public void reset() {
        left.reset();
        right.reset();
    }


    /**
     * constructs a JoinOperator
     *
     * @param exp   an Expression representing the join condition
     * @param left  the left child Operator
     * @param right the right child Operator
     */
    public JoinOperator(Expression exp, Operator left, Operator right) {
        this.exp = exp;
        this.left = left;
        this.right = right;
//
//        l  = left.getNextTuple();
//
//        r  = right.getNextTuple();

        List<String> newList = new ArrayList<>();
        newList.addAll(left.schema);
        newList.addAll(right.schema);
        this.schema = newList;

        jv = new JoinVisitors(left.schema,right.schema);

    }

    /**
     * concatenates two tuples together in order to perform a join
     * l: [1,2] and r:[200,300]
     * l: [3,4] and r:[300,400]
     * return
     * [1,2,200,300]
     * [1,2,300,400]
     * [3,4,200,300]
     * [3,4,300,400]
     *
     * @param l the left tuple
     * @param r the right tuple
     * @return  the new tuple formed by concatenating the left and right tuples
     */


    /**
     * checks whether two tuples satisfy the join condition
     *
     * @param l the left tuple
     * @param r the right tuple
     * @return  true or false depending on whether the tuples satisfy the condition
     */
    public boolean satisfy(Tuple l, Tuple r) {

        jv.setTuple(l,r);

        exp.accept(jv);
        return jv.getCondition();
    }

    /**
     * Nested Join loop algorithm
     * reset() to top whenever inner loop is null
     *
     * @return qualified tuple
     */
    public abstract Tuple getNextTuple();

}
