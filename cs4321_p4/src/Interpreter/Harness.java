package Interpreter;

import java.io.*;

import jnio.Converter;
import jnio.TupleWriter;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;

import org.junit.Test;
import java.io.FilenameFilter;

import util.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;


/**
 * Harness is our top-level interpreter that reads the input and produces the output
 *
 * @authors Yao Xiao, Kyle Johnson, Valerie Tan
 */
public class Harness {

    private static class HarnessConfig {
        private String inPath;
        private String outPath;
        private String tempPath;
        private boolean BuildIdx = true;
        private boolean Evaluate = true;
        private boolean BuildStats = true;

        public HarnessConfig(String configPath) throws IOException {
            BufferedReader br =  new BufferedReader(new FileReader(configPath));
            inPath = br.readLine();
            outPath = br.readLine();
            tempPath = br.readLine();
            br.close();


        }

    }

    /**
     * Run the sql interpreter using the specified configuration file path.
     * This method is designed for the requirements of project 4.
     *
     * @param configPath the path of the configuration file.
     * @throws IOException If an I/O error occurs.
     */
    public void execute(String configPath) throws IOException {
        HarnessConfig config = new HarnessConfig(configPath);
        DBCatalog.setDBCatalog(config.inPath, config.outPath, config.tempPath);
        DBCatalog.getDB();

        if (config.BuildIdx) {
            indexConfig.buildIndex();
        }

        if (config.Evaluate) {
            processQuery(config.inPath, config.outPath, config.tempPath);
        }

    }


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

    public void processQuery(String inputPath, String outputPath, String tempdir) {


        try{
            CCJSqlParser parser = new CCJSqlParser(new FileReader(DBCatalog.querydir));
            Statement statement;
            int counter = 1;
            while((statement = parser.Statement()) !=null ) {
                String out  = DBCatalog.outputdir + "query" + counter;
                TupleWriter writer = new TupleWriter(out);
                File logicalPlan = new File(DBCatalog.outputdir
                        + File.separator + "query" + counter + "_logicalplan");
                File physicalPlan = new File(DBCatalog.outputdir
                        + File.separator + "query" + counter + "_physicalplan");
                PrintStream logicalPlanStream = new PrintStream(logicalPlan);
                PrintStream physicalPlanStream = new PrintStream(physicalPlan);
                System.out.println("Parsing: " + statement);
                Selector select = new Selector(statement);
                select.root.printTree(physicalPlanStream, 0);
                physicalPlanStream.close();
                select.logicalRoot.printTree(logicalPlanStream, 0);
                logicalPlanStream.close();
                long beginTime = System.currentTimeMillis();

                select.root.dump(writer);
                long endTime = System.currentTimeMillis();
                System.out.println("query "
                        + counter + " took " + (endTime - beginTime)*1.0/1000 + " seconds");
                counter++;
                writer.close();



            }
            int num = 1;
            while(num < counter) {
                Converter convert = new Converter(outputPath +"/query" + num);
                convert.binaryToReadable();
                num++;
            }



        } catch(Exception e) {
            e.printStackTrace();
        }
    }


//    public static void main(String args[]) throws IOException {
//        if(args.length < 1) {
//            throw new IllegalArgumentException("Not enough argument!");
//        }
//        Harness interpreter = new Harness();
//        interpreter.execute(args[0]);
//
//    }
    @Test
    public void main() throws IOException{
       Harness itpr = new Harness();
        itpr.execute("/Users/yaoxiao/Database-Practium/cs4321_p4/samples/interpreter_config_file.txt");
       // itpr.harness("/Users/yaoxiao/Database-Practium/cs4321_p2/samples/input","/Users/yaoxiao/Database-Practium/cs4321_p2", "/Users/yaoxiao/Database-Practium/cs4321_p2");

    }
}
