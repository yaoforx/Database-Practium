package btree;

import util.Table;
import util.TupleIdentifier;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

public class Btree {
    private boolean clustered;
    private File indexfile;
    private int order;
    private BTreeNode root;
    public ArrayList<ArrayList<BTreeNode>> layers;
    public int numOfLeaves;
    private Table table;
    private String indexedCol;
    private int pageSize = 4096;


    /**
     * Constructor for the B+ tree
     * @param table the table to build the index tree
     * @param colName the column name for indexing
     * @param cluster true if it is a cluster tree
     * @param order order of the tree
     */
    public Btree(Table table, String colName, int cluster, int order, File indexfile) {
        this.table = table;
        this.indexedCol = colName;
        this.clustered = cluster == 1;
        this.order = order;
        this.root = null;
        this.indexfile = indexfile;
        layers = new ArrayList<ArrayList<BTreeNode>> ();

    }
    public BTreeNode getRoot(){
        return root;
    }
    public boolean isClustered(){
        return clustered;
    }
    public String getIndexedCol(){
        return indexedCol;
    }

    /**
     * Serializes
     */
    public void serialize() {
        if(root == null) {
            throw new RuntimeException("The tree does not exists");

        }
        try{
            FileOutputStream fos = new FileOutputStream(indexfile);
            FileChannel fc = fos.getChannel();
            ByteBuffer buf = ByteBuffer.allocate(pageSize);
            setZeros(buf);
            buf.putInt(0, root.getSize());
            buf.putInt(4, root.leafNum());
            buf.putInt(8, order);
            fc.write(buf);
            for(ArrayList<BTreeNode> layer : layers) {
                serializeLayer(layer,buf, fc);
            }
            fc.close();
            fos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    private void serializeLayer(ArrayList<BTreeNode> layer, ByteBuffer buf, FileChannel fc) {
        for(BTreeNode node : layer) {
            setZeros(buf);
            node.serialize(buf);
            try {
                fc.write(buf);
            } catch (IOException e) {
                System.err.println("Serializing failed on node " + node.addr);
                e.printStackTrace();
            }
        }
    }

    /**
     * Deserialize the b+ tree and set up the root
     */
    public void deserialize(){
        try {
            FileInputStream fis = new FileInputStream(indexfile);
            FileChannel fc = fis.getChannel();
            ByteBuffer buffer = ByteBuffer.allocate(pageSize);
            fc.read(buffer);
            int rootAddr = buffer.getInt();
            numOfLeaves = buffer.getInt();
            int serialOrder = buffer.getInt();
            if(serialOrder != order) throw new RuntimeException("Order does not match");
            root = deserializeNode(fc, buffer, rootAddr, null);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public BTreeNode deserializeNode(FileChannel fc, ByteBuffer buffer, int page, Integer traversekey) {
        read(fc, buffer, page);
        boolean isIndexNode = buffer.getInt(0) == 1;
        if(isIndexNode) {
            return deserializeIndexNode(fc, buffer, page, traversekey);
        }
        else return deserializeLeafNode(page, buffer);

    }
    private BTreeNode deserializeIndexNode(FileChannel fc, ByteBuffer buffer, int page, Integer traversekey) {
        int numKeys = buffer.getInt(4);
        List<Integer> keys = new ArrayList<>(numKeys);
        List<Integer> pointers = new ArrayList<>(numKeys + 1);
        int pos = 8;
        int traverseIdx = numKeys;
        for(int i = 0; i < numKeys; i++) {
            keys.add(buffer.getInt(pos));
            if(traversekey != null) {
                if(traversekey < keys.get(i)) {
                    traverseIdx = i;
                }
            }
            pos += 4;
        }
        for(int i = 0; i < numKeys + 1; i++) {
            pointers.add(buffer.getInt(pos));
            pos += 4;
        }
        if(traversekey == null) {// we arr only exploring current node and its children
            List<BTreeNode> childs = new ArrayList<BTreeNode>(pointers.size());

            for(int i = 0; i < childs.size(); i++)
                childs.add(deserializeNode(fc, buffer, pointers.get(i), traversekey));
            return new BtreeIndexNode(keys, childs, page, order);
        } else {
            return deserializeNode(fc, buffer, pointers.get(traverseIdx), traversekey);
        }
    }
    private BTreeNode deserializeLeafNode(int page, ByteBuffer buffer) {
        int numDataEntry = buffer.getInt(4);
        int pos = 8;
        List<Integer> keys = new ArrayList<>();
        List<ArrayList<TupleIdentifier>> entries = new ArrayList<ArrayList<TupleIdentifier>>();
        for(int i = 0; i < numDataEntry; i++) {
            keys.add(buffer.getInt(pos));
            pos += 4;
            int numOfTps = buffer.getInt(pos);
            pos += 4;
            ArrayList<TupleIdentifier> tuples = new ArrayList<>();
            for(int j = 0;j < numOfTps; j++) {
                int pageNum = buffer.getInt(pos);
                pos += 4;
                int tupleNum = buffer.getInt(pos);
                pos += 4;
                tuples.add(new TupleIdentifier(pageNum, tupleNum));
            }
            entries.add(tuples);
        }
        return new BtreeLeafNode(page, keys, entries, order);

    }

    /**
     * Function to create the Leaf nodes layer
     * @param sortedMap
     * @return a list of leaf nodes
     */
    public ArrayList<BTreeNode> createLeafLayer(TreeMap<Integer, ArrayList<TupleIdentifier>> sortedMap) {
        List<BTreeNode> leaves = new ArrayList<>();
        int totalKeys = sortedMap.size();
        int currentAddr = 1;
        int keyLeft = totalKeys;
        int keyPos = 0;
        //There are more keys to process and we are not under-filling from second to last
        while(keyLeft > 0 && !(keyLeft > 2 * order && keyLeft < 3 * order)) {
            List<ArrayList<TupleIdentifier>> entries = new ArrayList<>();
            int filling = Math.min(2 * order, keyLeft);
            leaves.add(createLeafNodes(filling,keyPos, sortedMap, currentAddr));
            keyPos += filling;
            keyLeft -= filling;
            currentAddr++;
        }
        //dealing with last two nodes. Arrange the filling method to make them valid.
        if(keyLeft > 2 * order && keyLeft < 3 * order) {
            //Divide the remaining keys two both sides
            int onLeft = keyLeft/2;
            int onRight = keyLeft - onLeft;
            leaves.add(createLeafNodes(onLeft, keyPos, sortedMap, currentAddr));
            keyPos += onLeft;
            currentAddr++;
            leaves.add(createLeafNodes(onRight, keyPos, sortedMap, currentAddr));
        }
        return (ArrayList<BTreeNode>) leaves;

    }

    /**
     * Helper function to create a leaf node based on index and size of a node
     * @param size
     * @param idx
     * @param sortedMap
     * @param currentAddr
     * @return a leaf node
     */
    private BtreeLeafNode createLeafNodes(int size, int idx,TreeMap<Integer, ArrayList<TupleIdentifier>> sortedMap, int currentAddr ) {
        List<Integer> keys = new ArrayList<>();
        List<ArrayList<TupleIdentifier>> entries = new ArrayList<>();
        for(int i = 0; i < size; i++) {
            keys.add((Integer) sortedMap.keySet().toArray()[idx]);
            entries.add(sortedMap.get(idx));
            idx++;
        }
        return new BtreeLeafNode(currentAddr, keys, entries, order);

    }

    /**
     * Create an index nodes layer
     * @param currentAddr
     * @param childlayer
     * @return a list of Btree nodes
     */
    public ArrayList<BTreeNode> createIndexLayer(int currentAddr, List<BTreeNode> childlayer) {
        int childrenSize = childlayer.size();
        int childRemain = childrenSize;
        int childPos = 0;
        List<BTreeNode> indexes = new ArrayList<>();
        while(childRemain > 0 && !(childRemain > 2 * order + 1 && childRemain < 3 * order + 2)) {
            int size = Math.min(2 * order + 1, childRemain);
            indexes.add(createIndexNodes(size, childPos, childlayer, currentAddr));
            currentAddr++;
            childPos += size;
            childRemain -= size;
        }
        if(childRemain > 2 * order +1 && childRemain < 3 * order + 2) {
            int onLeft = childRemain/2;
            int onRight = childRemain - onLeft;
            indexes.add(createIndexNodes(onLeft, childPos, childlayer, currentAddr));
            currentAddr++;
            childPos += onLeft;
            indexes.add(createIndexNodes(onRight, childPos, childlayer, currentAddr));

        }
        return (ArrayList<BTreeNode>) indexes;
    }

    /**
     * Helper function to create a index node based on index and size of a node
     * @param size
     * @param idx
     * @param childlayer
     * @param addr
     * @return a index node
     */
    private BtreeIndexNode createIndexNodes(int size, int idx, List<BTreeNode> childlayer, int addr) {
        List<Integer> keys = new ArrayList<>();
        List<BTreeNode> children = new ArrayList<>();
        for(int i = 0; i < size; i++) {
            children.add(childlayer.get(idx));
        }
        for(int i = 0; i < keys.size(); i++) {
            if(children.get(i+1) != null) {
                keys.add(children.get(i + 1).getSmallest());
            }
        }
        return new BtreeIndexNode(keys, children, addr, order);
    }

    /**
     * Read a page(node) at specific page number
     * @param fc
     * @param buffer
     * @param pageNum
     */
    private void read(FileChannel fc, ByteBuffer buffer, int pageNum) {
        setZeros(buffer);
        try {
            int pos = pageSize * pageNum;
            fc.position(pos);
            fc.read(buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void setZeros(ByteBuffer buffer) {
        buffer.clear();
        buffer.put(new byte[pageSize]);
        buffer.clear();
    }
    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("Index on %s\n", indexedCol));
        Queue<BTreeNode> lastLayer = new LinkedList<BTreeNode>();
        Queue<BTreeNode> nextLayer = new LinkedList<BTreeNode>();
        BTreeNode current = null;
        lastLayer.add(root);
        while(lastLayer.peek() != null) {
            while((current = lastLayer.poll()) != null) {
                current.printTree(sb);
                List<BTreeNode> children = current.getChildren();
                if(children != null) {
                    for(BTreeNode child : children) {
                        nextLayer.add(child);
                    }
                }
            }
            sb.append("\n");
            lastLayer = nextLayer;
            nextLayer = new LinkedList<BTreeNode>();
        }
        return sb.toString();
    }
    public BTreeNode getLeafNode(int idx) {
        //smallest lead node will always on page 1
        int page = idx;
        try {
            FileInputStream fis = new FileInputStream(indexfile);
            FileChannel channel = fis.getChannel();
            ByteBuffer buffer = ByteBuffer.allocate(4096);
            channel.read(buffer);
            numOfLeaves = buffer.getInt(4);
            //smallest lead node will always on page 1
            BTreeNode res = deserializeNode(channel, buffer, page, null);
            fis.close();
            return res;
        } catch (IOException e) {

            e.printStackTrace();
            return null;
        }
    }
    /**
     * A helper function to deserialize an index file.
     * @param targetKey
     * @return BTreeLeafNode
     */
    public BtreeLeafNode deserializeTraversal(Integer targetKey) {
        try {
            FileInputStream fis = new FileInputStream(indexfile);
            FileChannel channel = fis.getChannel();
            ByteBuffer buffer = ByteBuffer.allocate(4096);
            numOfLeaves = buffer.getInt(4);
            channel.read(buffer);
            int rootAddress = buffer.getInt(0);
            BtreeLeafNode res = (BtreeLeafNode)deserializeNode(channel, buffer, rootAddress, targetKey);
            fis.close();
            return res;
        } catch (IOException e) {

            e.printStackTrace();
            return null;
        }
    }
    public void setRoot(BTreeNode node) {root = node;}




}
