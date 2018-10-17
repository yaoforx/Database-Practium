package Interpreter;

import java.io.*;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;

import org.junit.Test;
import util.*;


/**
 * Harness is our top-level interpreter that reads the input and produces the output
 *
 * @authors Yao Xiao, Kyle Johnson, Valerie Tan
 */
public class Harness {

    /**
     * sets the database catalog using the input and output directories
     * parses each query in queries.sql and makes a Selector for the query which will create the operator tree
     * calls dump on the root of the operator tree to write the output to a new file in the output directory
     *
     * @param inputPath  the input directory
     * @param outputPath the output directory
     * @see              DBCatalog
     * @see              Selector
     */
    public void harness(String inputPath, String outputPath) {
        DBCatalog.setDBCatalog(inputPath,outputPath);
        DBCatalog.getDB();
        try{
            CCJSqlParser parser = new CCJSqlParser(new FileReader(DBCatalog.querydir));
            Statement statement;
            int counter = 1;
            while((statement = parser.Statement()) !=null ) {

                PrintStream ps = new PrintStream(new BufferedOutputStream(
                        new FileOutputStream(DBCatalog.outputdir + "query" + counter)));
                System.out.println("Parsing: " + statement);
                Selector select = new Selector(statement);
                counter++;
                select.root.dump(ps);

                ps.close();
            }



        } catch(Exception e) {
            e.printStackTrace();
        }
    }

//
//    public static void main(String args[]) {
//        if(args.length < 2) {
//            throw new IllegalArgumentException("Not enough argument!");
//        }
//        Harness interpreter = new Harness();
//        interpreter.harness(args[0],args[1]);
//
//    }
    @Test
    public void main() {
       Harness itpr = new Harness();

        itpr.harness("/Users/yaoxiao/Documents/cs4321/cs4321_p1/samples/input","/Users/yaoxiao/Documents/cs4321/cs4321_p1");

    }
}
