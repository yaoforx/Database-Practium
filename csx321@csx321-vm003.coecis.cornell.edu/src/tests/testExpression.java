package tests;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.select.*;
import operators.ScanOperator;
import org.junit.Test;
import util.DBCatalog;
import static org.junit.Assert.*;
//import static org.junit.jupiter.api.Assertions.assertEquals;

import java.beans.Expression;
import java.io.*;

import net.sf.jsqlparser.parser.CCJSqlParser;
import visitors.AbstractVisitors;
import visitors.SelectVisitors;

public class testExpression {
    String query = null;
    boolean cond = true;
    private boolean testExpression(String query) {
        this.query = query;

        AbstractVisitors visitor = new SelectVisitors(null);

        try {
            CCJSqlParser parser = new CCJSqlParser(new StringReader(query));
            //System.out.println("hello");
            Statement statement;
            statement = parser.Statement();

            Select select = (Select) statement;
            PlainSelect ps = (PlainSelect) select.getSelectBody();
            System.out.println(ps.getWhere().toString());
            AndExpression exp = (AndExpression) ps.getWhere();
           // AndExpression and = (AndExpression) where;
            visitor.visit(exp);
            cond = visitor.getCondition();
            AndExpression and = exp;
           // System.out.println( and.getLeftExpression().toString());
           // System.out.println( and.getRightExpression().toString());
        } catch (Exception e ) {
            System.err.println("opps");
        }
        return cond;

    }
    @Test
    public void testNumberical() {
        String query = "select * from a WHERE 1 < 2 AND 3 = 17";
        assertEquals(false,testExpression(query));

        String query2 = "select * from a WHERE 1 = 1 AND 2 ！= 2";
        assertEquals(false,testExpression(query2));
        String query3 = "select * from a WHERE 2 > 3 AND 2 < 3";
        assertEquals(false,testExpression(query3));
    }

}
