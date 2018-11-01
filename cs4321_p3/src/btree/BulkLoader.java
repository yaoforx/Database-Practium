package btree;


import operators.IndexScanOperator;
import operators.Operator;
import util.Table;

import java.io.File;

public class BulkLoader {
    private File indexfile;
    private static final int pageSize = 4096;
    private Operator indexScan;
    private Btree btree;

    public BulkLoader(int clustered, File indexfile, Integer lowKey, Integer highKey, Table table, String IndexedCol, int order) {
        this.btree = new Btree(table, IndexedCol, clustered, order,indexfile);
        this.indexScan = new IndexScanOperator(table, lowKey, highKey, btree, indexfile);
        this.indexfile = indexfile;
    }
    private void bulkLoad() {
        
    }
}
