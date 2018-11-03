package btree;

import util.Tuple;
import util.TupleIdentifier;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class BtreeLeafNode extends BTreeNode {
    private List<ArrayList<TupleIdentifier>> dataEntries;

    public BtreeLeafNode(int addr, List<Integer> keys, List<ArrayList<TupleIdentifier>> data, int order) {
        super(addr, keys, order);
        this.dataEntries = new ArrayList<>(data);

    }
    @Override
    public boolean isLeafNode() {
        return true;
    }

    /**
     * get the tuple info specified by key and entry
     * @param key
     * @param entry
     * @return tuple identifier
     */
    public TupleIdentifier getTupleInfo(int key, int entry) {
        return dataEntries.get(key).get(entry);
    }
    @Override
    public List<BTreeNode> getChildren() {
        return null;
    }
    public int sizeOfEntry(int index) {
        return dataEntries.get(index).size();

    }

    /**
     * find smallest key in all entries
     * @return
     */
    @Override
    public int getSmallest() {
        int small = Integer.MAX_VALUE;
        for(Integer key : keys) {
            if(key != null && key < small) {
                small = key;
            }
        }
        return small;
    }

    /**
     * lead node size is 1
     * @return
     */
    @Override
    public int getSize() {
        return 1;
    }

    /**
     * lead node has one leaf
     * @return
     */
    @Override
    public int leafNum() {
        return 1;
    }

    /**
     * Serialize a leaf node to pages
     * @param buffer
     */
    @Override
    public void serialize(ByteBuffer buffer) {
        buffer.putInt(0,0);//0 represents leaf node
        buffer.putInt(4, dataEntries.size());
        int index = 8;

        for(int i = 0; i < keys.size(); i++) {
            List<TupleIdentifier> entry = dataEntries.get(i);
            buffer.putInt(index, keys.get(i));
            index += 4;
            buffer.putInt(index, entry.size());
            index += 4;
            for(TupleIdentifier info : entry) {
                buffer.putInt(index, info.getPageNum());
                index += 4;
                buffer.putInt(index, info.getTupleNum());
                index += 4;
            }
        }

    }





    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("LeafNode[\n");
        for(int i = 0; i < keys.size(); i++) {
            sb.append("<[" + keys.get(i) +":");
            for(int j = 0; j < dataEntries.get(i).size(); j++)
                sb.append(dataEntries.get(i).get(j));
            sb.append("]>\n");
        }
        sb.append("]\n");
        return sb.toString();
    }
}
