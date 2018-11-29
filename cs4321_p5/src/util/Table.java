package util;
import java.util.*;
import java.io.BufferedReader;
import java.io.IOException;

/**
 * Table class utilized by operators
 *
 * @authors Yao Xiao, Kyle Johnson, Valerie Tan
 */
public class Table {
    public String tableName = "";
    private List<String> columns = null;

    /**
     * constructs a Table
     *
     * @param name   the name of the table
     * @param schema the schema of the table (i.e. column names)
     */
    public Table(String name, List<String> schema) {
        this.tableName = name;
        this.columns = schema;


    }

    /**
     * @return the schema of this table
     */
    public List<String> getSchema(){
        return columns;
    }
}
