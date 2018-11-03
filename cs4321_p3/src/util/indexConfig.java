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

import net.sf.jsqlparser.schema.Table;
public class indexConfig {
    public static HashMap<String, BulkLoader> loaders;
    public indexConfig(){
        loaders = new HashMap<>();
    }

    public static void buildIndex() throws IOException{


      //  File idxFile = new File(DBCatalog.indexdir);
        for(String set : DBCatalog.indexes.keySet()) {
            String tabPath = DBCatalog.dbdir + set;
            indexInfo info = DBCatalog.indexes.get(set);
            int idxCol = DBCatalog.schemas.get(set).indexOf(info.indexCol);
            String idxedPath = DBCatalog.indexdir + set + "." + info.indexCol;

            //if the index is to be clustered, start by sorting the relation on the desired attribute and
            //replacing the old (unsorted) relation le with the new (sorted) relation le. Then build the index.

            if(info.clustered) {
                PhysicalPlanBuilder builder = new PhysicalPlanBuilder();
                Table table = new Table(null, set);
                OrderByElement obe = new OrderByElement();
                obe.setExpression(new Column(table, info.indexCol));
                LogicalOperator log = new LogicalScan(DBCatalog.getTable(set));
                log = new LogicalSort(Arrays.asList(obe),log);
                log.accept(builder);
                Operator root = builder.getRoot();
                try{
                    tabPath += "_clustered";
                    TupleWriter tw = new TupleWriter(tabPath);
                    root.dump(tw);
                    tw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
            System.out.println("Curretnly is building index for table "+ idxedPath);

            File indexout = new File(idxedPath);
            BulkLoader bulkLoader = new BulkLoader(info.clustered, indexout, idxCol, info.order, new File(tabPath));
            if(!loaders.containsKey(set)) loaders.put(set, bulkLoader);
            bulkLoader.getBtree().serialize();
            PrintStream ps = new PrintStream(new File(idxedPath + "_humanreadableTest"));
            bulkLoader.getBtree().dump(ps);

        }

    }
}
