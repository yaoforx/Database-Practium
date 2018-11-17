package util;

import jnio.TupleReader;

import java.io.*;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.HashMap;
import java.util.*;

/**
 * DBCatalog is the database catalog that is handling information for every table. it is is a
 * global entity and can be accessed by other components
 *
 * @authors Yao Xiao, Kyle Johnson, Valerie Tan
 */
public class DBCatalog {
    public static DBCatalog db = new DBCatalog();

    private DBCatalog(){}

    public static String inputdir;
    public static String outputdir;
    public static String schemadir;
    public static String dbdir;
    public static String statDir;
    public static String querydir;
    public static Configure config;
    public static String tempdir;
    public static String indexdir;
    public static String indexInfo;
    public static indexConfig idxConfig;


    /**
     * @return the database catalog
     */
    public static DBCatalog getDB() {return db;}

    public static HashMap<String,List<String>> schemas =  new HashMap<String,List<String>>();
    //alias key is alia, value is table
    public static HashMap<String,String> alias = new HashMap<>();
    //key is table name, value is <column Name, indexInfo>
    public static HashMap<String, HashMap<String, indexInfo>> indexes = new HashMap<>();
    //tablestats key is tableFullName, TableStat contains column name only
    public static HashMap<String, TableStat> tablestats = new HashMap<>();
    /**
     * sets the DBCatalog with the given input and output directories
     *
     * @param input  the input directory
     * @param output the output directory
     */
    public static void setDBCatalog(String input, String output, String temp) throws IOException {
        inputdir = input + "/";
        outputdir = output + "/";
        dbdir = inputdir + "db/data/";
        statDir  = inputdir + "db/";
        schemadir = input + "/db/" + "schema.txt";
        querydir = inputdir + "/queries.sql";
       tempdir = temp + "/";

        config = new Configure(inputdir + "plan_builder_config.txt");
        indexdir = input + "/db/" + "indexes/";
        indexInfo = input + "/db/" + "index_info.txt";
        createSchema();
        createIndexInfo();

        tablestats = (new Stats()).stats;

        idxConfig = new indexConfig();

    }

    /**
     * create schema Hash table to store tables' schema
     * key: table name
     * value: cols' name
     */
    public static void createSchema() {
        BufferedReader bf;
        try {
            bf = new BufferedReader(new FileReader(schemadir));
            String line;
            while((line =bf.readLine()) != null) {
                List<String> cols = Arrays.asList(line.split(" "));
                schemas.put(cols.get(0), cols.subList(1,cols.size()));
            }
            bf.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void createIndexInfo(){
        indexes.clear();

        try {
            BufferedReader br = new BufferedReader(new FileReader(indexInfo));
            String line = null;
            while((line = br.readLine()) != null) {
                String[] info = line.split(" ");
                if(info.length != 4) {
                    throw new RuntimeException("indexed has wrong attributes");
                }
                String tab = info[0];
                String col = info[1];
                boolean cluster = (info[2].equals("1"));
                int order = Integer.valueOf(info[3]);
                if(indexes.containsKey(tab)) {
                    indexes.get(tab).put(col, new indexInfo(tab,col,cluster,order));
                }
                else {
                    HashMap<String, indexInfo> map = new HashMap<>();
                    map.put(col, new indexInfo(tab,col,cluster,order));
                    indexes.put(tab, map);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * @param tableName the name of the table whose schema is being retrieved
     * @return          the schema of the table named "tableName"
     *                  null if tableName does not refer to a table in DBCatalog
     */
    public static List<String> getSchema(String tableName) {
        tableName = Util.getFullTableName(tableName);
        if(!schemas.containsKey(tableName)) return null;
        return schemas.get(tableName);
    }

    /**
     * @param tableName the name of the table that is being retrieved
     * @return          the table named "tableName"
     *                  null if tableName does not refer to a table in DBCatalog
     */
    public static Table getTable(String tableName) {
        String realname= null;
        if(DBCatalog.alias.containsKey(tableName)) {
            realname = DBCatalog.alias.get(tableName);
        }
        String readFrom = realname == null ? tableName : realname;
        TupleReader br = tableReader(readFrom);
        if (br == null) return null;
        return new Table(tableName, getSchema(readFrom));
    }

    /**
     * create a BufferedReader for table "fileName"
     * utilized by reset() in operators
     *
     * @param fileName the name of the table for which a BufferedReader is being created
     * @return         a BufferedReader starting at the beginning of table "fileName"
     *                 null if fileName does not refer to a table in DBCatalog
     */
//    public static BufferedReader tableReader(String fileName) {
//        String tablePath;
//        if(alias.containsKey(fileName)) {
//            fileName = alias.get(fileName);
//            tablePath =dbdir + fileName ;
//        }
//        else {
//            tablePath = dbdir + fileName;
//        }
//        try{
//            BufferedReader bf = new BufferedReader(new FileReader(tablePath));
//            return bf;
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        }
//        return null;
//    }
    public static TupleReader tableReader(String fileName) {
        String tablePath;
        if(alias.containsKey(fileName)) {
            fileName = alias.get(fileName);
            tablePath =dbdir + fileName ;
        }
        else {
            tablePath = dbdir + fileName;
        }
        try{
            TupleReader bf = new TupleReader(new File(tablePath));
            return bf;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
    public static HashMap<String, indexInfo> getindexInfo(String tableName) {

        return indexes.get(tableName);
    }

    public static indexInfo getindexInfo(String tableName, String colName) {
        if(alias.containsKey(tableName)) tableName = alias.get(tableName);
        if (indexes != null && indexes.containsKey(tableName)){
            if (indexes.get(tableName).containsKey(colName)){
                return indexes.get(tableName).get(colName);
            }
        }
        return null;
    }


}
