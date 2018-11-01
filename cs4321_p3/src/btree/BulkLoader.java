package btree;


import jnio.TupleReader;
import jnio.TupleWriter;
import operators.IndexScanOperator;
import operators.Operator;
import operators.SortInMemory;
import operators.SortOperator;
import util.DBCatalog;
import util.Table;
import util.Tuple;
import util.TupleIdentifier;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

public class BulkLoader {
    private File indexfile;
    private static final int pageSize = 4096;
    private Operator indexScan;
    private Btree btree;
    private String IndexedCol;
    private File input;
    private TupleReader tr;

    public BulkLoader(int clustered, File indexfile, Integer lowKey, Integer highKey, Table table, String IndexedCol, int order, File input) {
        this.indexfile = indexfile;
        this.btree = new Btree(table, IndexedCol, clustered, order,indexfile, input);
        this.indexScan = new IndexScanOperator(table, lowKey, highKey, btree, indexfile);

        this.IndexedCol = IndexedCol;

    }
    private void bulkLoad() {
        TreeMap<Integer, ArrayList<TupleIdentifier>> entryMap = new TreeMap<>();
        try {
            // tuples in this page.
            ArrayList<Tuple> tps;
            int currPageId = 0;
            while ((tps = tr.readNextPage()) != null) {
                for (int currTupleId = 0; currTupleId < tps.size(); currTupleId++) {
                    Tuple currTuple = tps.get(currTupleId);
                    int key = currTuple.tuple.get(getColIdx(IndexedCol, indexScan.schema));
                    if (entryMap.containsKey(key)) {
                        ArrayList<TupleIdentifier> target = entryMap.get(key);
                        target.add(new TupleIdentifier(currPageId, currTupleId));
                    } else {
                        ArrayList<TupleIdentifier> newEntry = new  ArrayList<>();
                        newEntry.add(new TupleIdentifier(currPageId, currTupleId));
                        entryMap.put(key, newEntry);
                    }
                }

                //finished one page, increment the page id.
                currPageId++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        ArrayList<BTreeNode> leaves = btree.createLeafLayer(entryMap);
        btree.layers.add(leaves);
        if(leaves.size() == 1) {
            btree.setRoot(leaves.get(0));
            return;
        }
        int currentNodeAddr = leaves.size() + 1;

        ArrayList<BTreeNode> indexLayer = btree.createIndexLayer(currentNodeAddr, leaves);
        btree.layers.add(indexLayer);
        currentNodeAddr += indexLayer.size();
        while(indexLayer.size() > 1) {
            indexLayer = btree.createIndexLayer(currentNodeAddr, indexLayer);
            btree.layers.add(indexLayer);
        }
        btree.setRoot(indexLayer.get(0));


    }

    static public int getColIdx(String element, List<String> schema) {
        int idx = schema.indexOf(element);

        if (idx != -1) {
            return idx;
        }

        else {

            for (int i = 0; i < schema.size(); i++) {
                String col = schema.get(i);
                col = col.split("\\.")[1];

                if (col.equals(element.toString().split("\\.")[1])) {
                    return i;
                }
            }

        }
        return -1;
    }
}
