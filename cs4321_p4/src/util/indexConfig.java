package util;

import btree.BulkLoader;
import jnio.TupleWriter;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;
import operators.LogicalOperator;
import operators.LogicalScan;
import operators.LogicalSort;
import operators.Operator;
import visitors.PhysicalPlanBuilder;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashMap;
/**
 * Index configuration class to construct b+trees based on index_info
 * @author Yao Xiao
 */
import net.sf.jsqlparser.schema.Table;
public class indexConfig {
    /**
     * HashMap key is table name and value are Bulkloaders
     * Every table has a bulkloader if it is indexed
     * BulkLoader see btree/BulkLoader.java
     */
    public static HashMap<String, BulkLoader> loaders;
    public indexConfig(){
        loaders = new HashMap<>();
    }

    /**
     * Buid b+tree and dump results
     * @throws IOException
     */
    public static void buildIndex() throws IOException{


      //  File idxFile = new File(DBCatalog.indexdir);
        System.out.println("FINd me here");
        System.out.println(DBCatalog.indexes.keySet().size());
        for(String set : DBCatalog.indexes.keySet()) {
            String tabPath = DBCatalog.dbdir + set;
            HashMap<String, indexInfo> info = DBCatalog.indexes.get(set);
            for(String indexedCol : info.keySet()) {
                int idxCol = DBCatalog.schemas.get(set).indexOf(indexedCol);
                String idxedPath = DBCatalog.indexdir + set + "." + indexedCol;

                /**
                 * if the index is to be clustered, start by sorting the relation on the desired attribute and
                 * replacing the old (unsorted) relation le with the new (sorted) relation le. Then build the index.
                 */

                if (info.get(indexedCol).clustered) {
                    PhysicalPlanBuilder builder = new PhysicalPlanBuilder();
                    Table table = new Table(null, set);
                    OrderByElement obe = new OrderByElement();
                    obe.setExpression(new Column(table, indexedCol));
                    LogicalOperator log = new LogicalScan(DBCatalog.getTable(set));
                    log = new LogicalSort(Arrays.asList(obe), log);
                    log.accept(builder);
                    Operator root = builder.getRoot();
                    try {
                        tabPath += "_clustered";
                        TupleWriter tw = new TupleWriter(tabPath);
                        root.dump(tw);
                        tw.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }

                System.out.println("Curretnly is building index for table " + idxedPath);

                File indexout = new File(idxedPath);
                indexInfo tableIndex = info.get(indexedCol);
                BulkLoader bulkLoader = new BulkLoader(tableIndex.clustered, indexout, idxCol,
                        tableIndex.order, new File(tabPath));
                if (!loaders.containsKey(set)) loaders.put(set, bulkLoader);
                bulkLoader.getBtree().serialize();
                PrintStream ps = new PrintStream(new File(idxedPath + "_humanreadable"));
                bulkLoader.getBtree().dump(ps);
                info.get(indexedCol).setUpLeafNumber(bulkLoader.getBtree().numOfLeaves);
            }

        }

    }
}
