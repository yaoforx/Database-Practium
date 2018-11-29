package btree;


import jnio.TupleReader;

import util.Tuple;
import util.TupleIdentifier;

import java.io.File;
import java.io.IOException;

import java.util.*;

/**
 * Bulk load Class to load input file and process it as an index file
 * Construct a b+tree
 * @author Yao Xiao
 */
public class BulkLoader {
    private static final int pageSize = 4096;
    private Btree btree;
    private Integer IndexedCol;
    private File input;
    private TupleReader tr;
    private boolean isclustered;

    /**
     * Constructor for BulkLoader
     * @param clustered if it should be indexed
     * @param indexfile out put indexed file
     *
     * @param IndexedCol the column that indexed on/
     *                  one table only has one coln
     * @param order B+ tree order
     * @param input input file
     */
    public BulkLoader(boolean clustered, File indexfile, Integer IndexedCol, int order, File input) throws IOException {
        this.btree = new Btree(IndexedCol, clustered, order,indexfile, input);
        this.isclustered = clustered;
        this.IndexedCol = IndexedCol;
        this.tr = new TupleReader(input);
        this.input = input;
        bulkLoad();

    }

    /**
     * bulk load function which to process indexed tuples and assign pageId and tupleId
     * also Calling Btree class to construct b+tree
     */
    private void bulkLoad() {
        TreeMap<Integer, ArrayList<TupleIdentifier>> entryMap = new TreeMap<>();

        try {
            // tuples in this page.
            ArrayList<Tuple> tps;
            int currPageId = 0;
            while ((tps = tr.readNextPage()) != null) {
                for (int currTupleId = 0; currTupleId < tps.size(); currTupleId++) {
                    Tuple currTuple = tps.get(currTupleId);

                    int key = currTuple.tuple.get(IndexedCol);
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
        //System.out.println("Leaf layer: "+leaves.toString());
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
           // System.out.println("adding index layer: "+indexLayer.toString());
            indexLayer = btree.createIndexLayer(currentNodeAddr, indexLayer);
            btree.layers.add(indexLayer);
           // System.out.print(indexLayer.toString());
        }
        btree.setRoot(indexLayer.get(0));


    }
    public Btree getBtree(){
        return btree;
    }


}
