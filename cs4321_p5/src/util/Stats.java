package util;

import jnio.TupleReader;

import java.io.*;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.CompletableFuture.runAsync;

/**
 * Stats class contains information for each realtion
 * @author yao xiao
 */
public class Stats {
    File stat;
    String outputDir;
    String inputDir;
    private BufferedReader br;
    private BufferedWriter bw;
    public static HashMap<String, TableStat> stats;

    public Stats() throws IOException {
        outputDir = DBCatalog.statDir;
        inputDir = DBCatalog.dbdir;
        stat = new File(inputDir + "/stat.txt");
        bw = new BufferedWriter(new FileWriter(stat));
        stats = new HashMap<>();
        parallelStats();
    }
    public void parallelStats() throws IOException{
        ExecutorService executorService = Executors.newFixedThreadPool(4);
        CompletableFuture[] futures = new CompletableFuture[DBCatalog.schemas.size()];
        int i = 0;
        for(String table : DBCatalog.schemas.keySet()) {
           Runnable task = () ->{
               try{
                   createStats(table);
               } catch (Exception e) {
                   e.printStackTrace();
               }
           };
           futures[i] = runAsync(task, executorService);
          i++;
        }
        executorService.shutdown();
        CompletableFuture.allOf(futures).join();


        writeStats();
        bw.close();
    }
    private void writeStats() throws IOException{
        for(String table : DBCatalog.schemas.keySet()) {
            int num = DBCatalog.schemas.get(table).size();
            bw.write(table + " ");
            int size = stats.size();
            bw.write(stats.get(table).totalTuple + " ");
            for(int i = 0; i < num; i++){ // not count table name
                String colName = DBCatalog.schemas.get(table).get(i);
                bw.write(colName+",");
                int[] ss = stats.get(table).indexRange.get(colName);
                String sss = table + "." + colName;
                bw.write( stats.get(table).indexRange.get(table + "." + colName)[0]+",");
                bw.write(stats.get(table).indexRange.get(table + "." + colName)[1] + " ");
            }
            bw.newLine();

        }
    }

    public void createStats(String table) throws IOException {

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
            for(int i = 0; i < num; i++){ // not count table name
                String s = table +"."+ DBCatalog.schemas.get(table).get(i);
                info.addCol(table +"."+ DBCatalog.schemas.get(table).get(i), min[i], max[i]);
                info.setTpNum(curtps);
            }
            stats.put(table, info);

    }

}
