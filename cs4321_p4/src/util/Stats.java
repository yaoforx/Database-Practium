package util;

import jnio.TupleReader;

import java.io.*;
import java.util.HashMap;

public class Stats {
    File stat;
    String outputDir;
    String inputDir;
    private BufferedReader br;
    private BufferedWriter bw;
    public HashMap<String, TableStat> stats;

    public Stats() throws IOException {
        outputDir = DBCatalog.statDir;
        inputDir = DBCatalog.dbdir;
        stat = new File(outputDir + "/stat.txt");
        bw = new BufferedWriter(new FileWriter(stat));
        createStats();
    }

    public void createStats() throws IOException {
        for(String table : DBCatalog.alias.values()) {
            int num = DBCatalog.schemas.get(table).size();
            TableStat info = new TableStat(table, new String[num], new int[num], new int[num]);
            int curtps = 0;
            int min[] = new int[num];
            int max[] = new int [num];
            for(int i = 0; i < min.length; i++) min[i] = Integer.MAX_VALUE;
            for(int i = 0; i < min.length; i++) max[i] = Integer.MIN_VALUE;
            File tab = new File(DBCatalog.dbdir + table);
            Tuple tp = null;
            TupleReader tr = new TupleReader(tab);
            while((tp = tr.read()) != null) {
                curtps++;
                for(int i = 0; i < min.length; i++) {
                    min[i] = Math.min(min[i], tp.getValue(i));
                    max[i] = Math.max(max[i], tp.getValue(i));
                }
            }
            bw.write(table + " ");
            bw.write(curtps + " ");
            for(int i = 0; i < num; i++){ // not count table name
                bw.write(DBCatalog.schemas.get(table).get(i)+",");
                bw.write(min[i-1]+",");
                bw.write(max[i-1] + " ");
                info.addCol(DBCatalog.schemas.get(table).get(i), min[i-1], max[i-1]);
                info.setTpNum(curtps);
            }
            stats.put(table, info);
            bw.newLine();
        }
        bw.close();
    }

}
