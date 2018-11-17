package visitors;


import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;

//TODO: comments for AbstractVisitors
/**
 * AbstractVisitors class implements ExpressionVisitor
 */
public class AbstractVisitors implements ExpressionVisitor {
    //This class only implements part of the expression
    //Thus only a value and boolean need to be supported
    protected long value = 0;
    protected boolean condition = false;

    /**
     * pass the final result to root
     *
     * @return condition
     */
    public boolean getCondition(){
        return condition;
    }

    /**
     *
     * @param expression
     */
    @Override
    public void visit(AndExpression expression){
        boolean leftCondition = false;
        boolean rightCondition = false;

        expression.getLeftExpression().accept(this);
    //    System.out.println(expression.getLeftExpression());
        leftCondition = condition;
        expression.getRightExpression().accept(this);
    //    System.out.println(expression.getRightExpression());

        rightCondition = condition;

        condition = (leftCondition && rightCondition);

      //  System.out.println(expression.getLeftExpression() + " AND " + expression.getRightExpression() + " is " + condition);
    }

    /**
     *
     * @param column
     */
    @Override
    public void visit(Column column) {

    }

    /**
     *
     * @param longValue
     */
    @Override
    public void visit(LongValue longValue) {
        value = longValue.getValue();
    }

    /**
     *
     * @param equalsTo
     */
    @Override
    public void visit(EqualsTo equalsTo) {
        long leftValue = 0;
        long rightValue = 0;
        equalsTo.getLeftExpression().accept(this);

        leftValue = value;

        equalsTo.getRightExpression().accept(this);

        rightValue = value;
        condition = (leftValue == rightValue);
       // System.out.println(equalsTo.getLeftExpression() + ": "+leftValue + " = "
       //         + equalsTo.getRightExpression() + ": "+ rightValue + " is " + condition);
//

    }

    /**
     *
     * @param greaterThan
     */
    @Override
    public void visit(GreaterThan greaterThan) {

        long leftValue = 0;
        long rightValue = 0;
        greaterThan.getLeftExpression().accept(this);
        leftValue = value;

        greaterThan.getRightExpression().accept(this);

        rightValue = value;

        condition = (leftValue > rightValue);
    }

    /**
     *
     * @param greaterThanEquals
     */
    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        long leftValue = 0;
        long rightValue = 0;
        greaterThanEquals.getLeftExpression().accept(this);
        leftValue = value;

        greaterThanEquals.getRightExpression().accept(this);

        rightValue = value;
       

        condition = (leftValue >= rightValue);

    }

    /**
     *
     * @param minorThan
     */
    @Override
    public void visit(MinorThan minorThan) {
        long leftValue = 0;
        long rightValue = 0;
        minorThan.getLeftExpression().accept(this);

        leftValue = value;



        minorThan.getRightExpression().accept(this);


        rightValue = value;


        condition = (leftValue < rightValue);
     //   System.out.println(leftValue + " < " + rightValue + " is " + condition);


    }

    /**
     *
     * @param minorThanEquals
     */
    @Override
    public void visit(MinorThanEquals minorThanEquals) {

        long leftValue = 0;
        long rightValue = 0;
        minorThanEquals.getLeftExpression().accept(this);
        leftValue = value;

        minorThanEquals.getRightExpression().accept(this);

        rightValue = value;

        condition = (leftValue <= rightValue);

    }

    /**
     *
     * @param notEqualsTo
     */
    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        long leftValue = 0;
        long rightValue = 0;
        notEqualsTo.getLeftExpression().accept(this);
        leftValue = value;

        notEqualsTo.getRightExpression().accept(this);

        rightValue = value;

        condition = (leftValue != rightValue);
     //   System.out.println(leftValue + " != " + rightValue + " is " + condition);

    }

    //Not in use, add them to prevent complaining
    @Override
    public void visit(NullValue nullValue) {

    }

    @Override
    public void visit(Function function) {

    }

    @Override
    public void visit(InverseExpression inverseExpression) {

    }

    @Override
    public void visit(JdbcParameter jdbcParameter) {

    }

    @Override
    public void visit(DoubleValue doubleValue) {

    }



    @Override
    public void visit(DateValue dateValue) {

    }

    @Override
    public void visit(TimeValue timeValue) {

    }

    @Override
    public void visit(TimestampValue timestampValue) {

    }

    @Override
    public void visit(Parenthesis parenthesis) {

    }

    @Override
    public void visit(StringValue stringValue) {

    }

    @Override
    public void visit(Addition addition) {

    }

    @Override
    public void visit(Division division) {

    }

    @Override
    public void visit(Multiplication multiplication) {

    }

    @Override
    public void visit(Subtraction subtraction) {

    }

    @Override
    public void visit(OrExpression orExpression) {

    }

    @Override
    public void visit(Between between) {

    }


    @Override
    public void visit(InExpression inExpression) {

    }

    @Override
    public void visit(IsNullExpression isNullExpression) {

    }

    @Override
    public void visit(LikeExpression likeExpression) {

    }



    @Override
    public void visit(SubSelect subSelect) {

    }

    @Override
    public void visit(CaseExpression caseExpression) {

    }

    @Override
    public void visit(WhenClause whenClause) {

    }

    @Override
    public void visit(ExistsExpression existsExpression) {

    }

    @Override
    public void visit(AllComparisonExpression allComparisonExpression) {

    }

    @Override
    public void visit(AnyComparisonExpression anyComparisonExpression) {

    }

    @Override
    public void visit(Concat concat) {

    }

    @Override
    public void visit(Matches matches) {

    }

    @Override
    public void visit(BitwiseAnd bitwiseAnd) {

    }

    @Override
    public void visit(BitwiseOr bitwiseOr) {

    }

    @Override
    public void visit(BitwiseXor bitwiseXor) {

    }


}
