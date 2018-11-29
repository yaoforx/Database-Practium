package optimal;

import net.sf.jsqlparser.schema.Column;
import unionfind.UnionFind;
import unionfind.UnionFindElement;
import util.Selector;
import util.Util;

import java.util.HashSet;

/**
 * Vsets Class holds information for v-value of each table and attributes
 * @author Yao Xiao
 */
public class Vsets {
    public static HashSet<Vvalues> sets;
    public Vsets(){

        sets =  new HashSet<Vvalues>();

    }
    public static boolean contains(String name) {
        for(Vvalues v : sets) {
            if (v.contains(name)) return true;
        } return false;
    }
    public static Double getVforTable(String name) {
        if(!sets.contains(name)) {
            sets.add(new Vvalues(name));

        }
        for(Vvalues v : sets) {
            if (v.contains(name)) return v.getV().get(name);
            String fullName = Util.getFullTableName(name);
            if (v.contains(fullName)) {
                return v.getV().get(fullName);
            }


        }

        return null;
    }



}