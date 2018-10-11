package tests;
import java.util.*;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.schema.*;
import operators.*;
import org.junit.Test;
import util.DBCatalog;
import static org.junit.Assert.*;

import util.Tuple;
import visitors.*;
import java.io.*;

import net.sf.jsqlparser.parser.CCJSqlParser;


/**
 * Created by yaoxiao on 9/23/18.
 */
public class testSelect {
    boolean cond = true;

    private boolean testSelectOperator(String query,List<Integer> cols) {

       // AbstractVisitors visitor = new SelectVisitors(null);
        Tuple tp = new Tuple(cols);
        DBCatalog.setDBCatalog("/Users/yaoxiao/Documents/cs4321/project1/samples/input","/Users/yaoxiao/Documents/cs4321/project1/samples/out/");
        SelectVisitors sl;
        try {
            CCJSqlParser parser = new CCJSqlParser(new StringReader(query));
            //System.out.println("hello");

            Statement statement;
            statement = parser.Statement();

            Select select = (Select) statement;
            PlainSelect ps = (PlainSelect) select.getSelectBody();

            FromItem tbName = ps.getFromItem();

            sl = new SelectVisitors(DBCatalog.getSchema(tbName.toString()));
            sl.setTuple(tp);
            EqualsTo equal = (EqualsTo) ps.getWhere();
            //Expression left = (Expression) and.getLeftExpression();


            sl.visit(equal);
            cond = sl.getCondition();

        } catch (Exception e ) {
            e.printStackTrace();
        }

        return cond;
    }

    @Test
    public void testNumberical() {
        String query = "select * from Sailors where Sailors.A = 1";
        List<Integer> cols = new ArrayList<>();
        cols.add(1);
        cols.add(200);
        cols.add(50);
        assertEquals(true,testSelectOperator(query,cols));


    }
    @Test
    public void testNumberical2() {

        List<Integer> cols = new ArrayList<>();
        cols.add(1);
        cols.add(200);
        cols.add(50);

        String query2 = "select * from Sailors where Sailors.A = 12";
        assertEquals(false,testSelectOperator(query2,cols));

    }



}
