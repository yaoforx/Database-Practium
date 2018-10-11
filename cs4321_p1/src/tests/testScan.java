package tests;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import operators.ScanOperator;
import org.junit.Test;
import util.DBCatalog;
import static org.junit.Assert.*;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.io.FileOutputStream;
import net.sf.jsqlparser.parser.CCJSqlParser;

/**
 * Created by yaoxiao on 9/23/18.
 */
public class testScan {

    private ScanOperator testScanOperator() {
        //Only testing the first queries in queries.sql!
        ScanOperator sop = null;
        //Please change the following to your own absolute path
        //not sure why but relative path does not work
        DBCatalog.setDBCatalog("/Users/yaoxiao/Documents/cs4321/project1/samples/input","/Users/yaoxiao/Documents/cs4321/project1/samples/out/");
        try {
            CCJSqlParser parser = new CCJSqlParser(new FileReader(DBCatalog.querydir));
            Statement statement;
            statement = parser.Statement();
            System.out.println(statement);
            if (statement == null) {
                throw new NullPointerException();
            }
            Select select = (Select) statement;
            PlainSelect ps = (PlainSelect) select.getSelectBody();
            FromItem from = ps.getFromItem();
            String tabName = from.toString();
            sop = new ScanOperator(DBCatalog.getTable(tabName));
        } catch (Exception e) {
            System.err.println("Died during parsing statements");
            e.printStackTrace();
        }
        return sop;
    }

    // test the function of get next tuple
    @Test
    public void testGetNextTuple() {
        // SELECT * FROM Sailors;
        System.out.println("Start to test scan functionality. ");
        ScanOperator scanOptr = testScanOperator();
        assertEquals("1,200,50", scanOptr.getNextTuple().toString());
        assertEquals("2,200,200", scanOptr.getNextTuple().toString());
        assertEquals("3,100,105", scanOptr.getNextTuple().toString());
        assertEquals("4,100,50", scanOptr.getNextTuple().toString());
        assertEquals("5,100,500", scanOptr.getNextTuple().toString());
        assertEquals("6,300,400", scanOptr.getNextTuple().toString());
        assertEquals(null, scanOptr.getNextTuple());
        System.out.println("Test Scan finished");
    }

    // test the function of reset operator.
    @Test
    public void testReset() {
        ScanOperator scanOptr = testScanOperator();
        System.out.println("Start to test reset functionality. ");
        //scanOptr.reset();
        assertEquals("1,200,50", scanOptr.getNextTuple().toString());
        scanOptr.reset();
        scanOptr.reset();
        assertEquals("1,200,50", scanOptr.getNextTuple().toString());
        assertEquals("2,200,200", scanOptr.getNextTuple().toString());
        assertEquals("3,100,105", scanOptr.getNextTuple().toString());
        assertEquals("4,100,50", scanOptr.getNextTuple().toString());
        assertEquals("5,100,500", scanOptr.getNextTuple().toString());
        assertEquals("6,300,400", scanOptr.getNextTuple().toString());
        assertEquals(null, scanOptr.getNextTuple());
        scanOptr.reset();
        //testing dump()
        PrintStream s = null;
        //Open out.txt to manually check
        try {
             s = new PrintStream(new FileOutputStream("out.txt", true));
            scanOptr.dump(s);
        } catch (IOException e) {
            e.printStackTrace();
        }
        s.close();

        System.out.println("Finishing testing reset");
    }

}
