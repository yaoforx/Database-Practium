//package operators;
//
//import jnio.TupleReader;
//import jnio.TupleWriter;
//import net.sf.jsqlparser.statement.select.OrderByElement;
//import util.Configure;
//import util.DBCatalog;
//import util.Tuple;
//
//import java.io.*;
//import java.util.*;
//import java.util.Random.*;
//public class ExternalSort extends SortOperator{
//    private String tempout;
//    private int id;
//
//    private int bufferPages = 0;
//    private Operator child;
//
//    public TupleReader reader = null;
//
//
//    public ExternalSort(Operator c, List<?> ties) {
//        super(c, ties);
//        Random random = new Random();
//        id = random.nextInt(1000);
//        bufferPages = DBCatalog.config.sortPage - 1;
//        this.child = c;
//
//
//        tempout = DBCatalog.tempdir;
//        this.child.reset();
//        sort();
//
//
//    }
//    @Override
//    public void reset() {
//        if (reader == null) return;
//        try {
//            reader.reset();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//    public void reset(int index) {
//       if(reader == null) return;
//            reader.reset(index);
//
//    }
//
//
//
//
//
//    @Override
//    public void reset(int pageNum, int index) {
//        if (reader == null) return;
//        try {
//            reader.reset(pageNum,index);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//    private String setName(int pass, int run) {
//        return tempout + Integer.toString(pass) + "#" + Integer.toString(run) + "_" + Integer.toString(id);
//    }
//    @Override
//    public Tuple getNextTuple() {
//        if (reader == null) return null;
//       try {
//           Tuple tp = reader.read();
//           return tp;
//       } catch (IOException e) {
//           e.printStackTrace();
//       }
//       return null;
//    }
//    private void sort() {
//
//
//
//        int tuplesInPage = 4096 /child.schema.size() / 4;
//        int tuplesInRun = tuplesInPage * bufferPages;
//        Tuple tp;
//        int run = 0;
//
//        while((tp = child.getNextTuple()) != null) {
//            List<Tuple> listToSort = new ArrayList<>(tuplesInRun);
//            int remain = tuplesInRun;
//            while(remain > 0 && tp != null) {
//                listToSort.add(tp);
//
//                tp = child.getNextTuple();
//
//                remain--;
//            }
//            if(tp != null) listToSort.add(tp);
//            Collections.sort(listToSort, compare);
//            try {
//               // System.out.println("Pass 0 file: " + setName(0, run));
//
//                TupleWriter tw = new TupleWriter(setName(0, run));
//                for(Tuple t : listToSort) {
//                    tw.write(t);
//
//                }
//                tw.close();
//
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//
//            run++;
//        }
//        if(run == 0) return;
//        merge(run);
//
//
//    }
//    private void merge(int Pass) {
//
//
//        int totalPass = Pass;
//        int pass = 0;
//        int outCount = Pass;
//
//
//        while(totalPass > 1) {
//           // System.out.println("Pass number " + totalPass);
//            int nextRun = 0;
//
//
//
//
//            List<TupleReader> buffer = new ArrayList<>();
//            int num = Math.min(outCount, bufferPages);
//
//            for(int i = 0; i < totalPass; i+= bufferPages) {
//                outCount = i/bufferPages;
//                num = Math.min(totalPass - i, bufferPages);
//
//
//                try {
//                    for (int j = i; j < i + num; j++) {
//
//                      //  System.out.println("Pass is " + pass + ", num is " + num + " Input file name is adding file: " + setName(pass, j));
//
//                        buffer.add(new TupleReader(new File(setName(pass, j))));
//
//                    }
//
//
//                   // System.out.println("output file name is adding file: " + setName(pass + 1, outCount));
//                    TupleWriter outputPage = new TupleWriter(setName(pass + 1, outCount));
//
//
//                    PriorityQueue<Tuple> pq = new PriorityQueue<>(compare);
//                    Tuple tuple = null;
//
//                    for (TupleReader tr : buffer) {
//                        Tuple tp = tr.read();
//                        if (tp != null) {
//                            tp.tupleReader = tr;
//                            pq.add(tp);
//
//                        }
//                    }
//
//                    while (!pq.isEmpty()) {
//                        Tuple tp = pq.poll();
//                        outputPage.write(tp);
//                        System.out.println(tp.toString());
//                        TupleReader tr = tp.tupleReader;
//                        tp = tr.read();
//                        if (tp != null) {
//                            tp.tupleReader = tr;
//                            pq.add(tp);
//                        }
//                    }
//
//
//
//
//                    for (int j = i; j < i + num; j++) {
//                        File file = new File(setName(pass , j));
//                    //    System.out.println("detleting " + setName(pass, j));
//                        file.delete();
//                    }
//
//                    buffer.clear();
//                    outputPage.close();
//                    nextRun++;
//
//
//
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//
//            }
//            pass++;
//
//
//            totalPass = nextRun;
//        }
//
//        //if(pass == 0) pass++;
//
//        try {
//            File orig = new  File(setName(pass, 0));
//            File result = new File(tempout + id + "_result");
//            orig.renameTo(result);
//            reader = new TupleReader(result);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//    }
//
//
//
//
//
//
//}

//package operators;
//
//import jnio.TupleReader;
//import jnio.TupleWriter;
//import net.sf.jsqlparser.statement.select.OrderByElement;
//import util.Configure;
//import util.DBCatalog;
//import util.Tuple;
//
//import java.io.*;
//import java.util.*;
//import java.util.Random.*;
//public class ExternalSort extends SortOperator{
//    private String tempout;
//    private int id;
//
//    private int bufferPages = 0;
//    private Operator child;
//
//    public TupleReader reader = null;
//
//
//    public ExternalSort(Operator child, List<?> ties) {
//        super(child, ties);
//        Random random = new Random();
//        id = random.nextInt(1000);
//        bufferPages = DBCatalog.config.sortPage - 1;
//        this.child = child;
//
//
//        tempout = DBCatalog.tempdir;
//
//        sort();
//
//
//    }
//    @Override
//    public void reset() {
//        if (reader == null) return;
//        try {
//            reader.reset();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//    @Override
//    public void reset(int index) {
//       if(reader == null) return;
//       try {
//
//           reader.reset(index);
//       }catch (IOException e) {
//           e.printStackTrace();
//       }
//
//    }
//
//
//
//
//    private String setName(int pass, int run) {
//        return tempout + Integer.toString(pass) + "#" + Integer.toString(run) + "_" + Integer.toString(id);
//    }
//    @Override
//    public Tuple getNextTuple() {
//        if (reader == null) return null;
//       try {
//           Tuple tp = reader.read();
//          // if(tp != null) System.out.println(tp.toString());
//           return tp;
//       } catch (IOException e) {
//           e.printStackTrace();
//       }
//       return null;
//    }
//    private void sort() {
//
//
//
//        int tuplesInPage = 4096 /child.schema.size() / 4;
//        int tuplesInRun = tuplesInPage * bufferPages;
//        Tuple tp;
//        int run = 0;
//
//        while((tp = child.getNextTuple()) != null) {
//            List<Tuple> listToSort = new ArrayList<>(tuplesInRun);
//            int remain = tuplesInRun;
//            while(remain > 0 && tp != null) {
//                listToSort.add(tp);
//
//                tp = child.getNextTuple();
//
//                remain--;
//            }
//            if(tp != null) listToSort.add(tp);
//            Collections.sort(listToSort, compare);
//            try {
//               // System.out.println("Pass 0 file: " + setName(0, run));
//
//                TupleWriter tw = new TupleWriter(setName(0, run));
//                for(Tuple t : listToSort) {
//                    tw.write(t);
//
//                }
//                tw.close();
//
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//
//            run++;
//        }
//        if(run == 0) return;
//        merge(run);
//
//
//    }
//    private void merge(int Pass) {
//
//
//        int totalPass = Pass;
//        int pass = 0;
//        int outCount = Pass;
//
//
//        while(totalPass > 1) {
//           // System.out.println("Pass number " + totalPass);
//            int nextRun = 0;
//
//
//
//
//            List<TupleReader> buffer = new ArrayList<>();
//            int num = Math.min(outCount, bufferPages);
//
//            for(int i = 0; i < totalPass; i+= bufferPages) {
//                outCount = i/bufferPages;
//                num = Math.min(totalPass - i, bufferPages);
//
//
//                try {
//                    for (int j = i; j < i + num; j++) {
//
//                      //  System.out.println("Pass is " + pass + ", num is " + num + " Input file name is adding file: " + setName(pass, j));
//
//                        buffer.add(new TupleReader(new File(setName(pass, j))));
//
//                    }
//
//
//                   // System.out.println("output file name is adding file: " + setName(pass + 1, outCount));
//                    TupleWriter outputPage = new TupleWriter(setName(pass + 1, outCount));
//
//
//                    PriorityQueue<Tuple> pq = new PriorityQueue<>(compare);
//                    Tuple tuple = null;
//
//                    for (TupleReader tr : buffer) {
//                        Tuple tp = tr.read();
//                        if (tp != null) {
//                            tp.tupleReader = tr;
//                            pq.add(tp);
//
//                        }
//                    }
//
//                    while (!pq.isEmpty()) {
//                        Tuple tp = pq.poll();
//                        outputPage.write(tp);
//                      //  System.out.println(tp.toString());
//                        TupleReader tr = tp.tupleReader;
//                        tp = tr.read();
//                        if (tp != null) {
//                            tp.tupleReader = tr;
//                            pq.add(tp);
//                        }
//                    }
//
//
//
//
//                    for (int j = i; j < i + num; j++) {
//                        File file = new File(setName(pass , j));
//                    //    System.out.println("detleting " + setName(pass, j));
//                        file.delete();
//                    }
//
//                    buffer.clear();
//                    outputPage.close();
//                    nextRun++;
//
//
//
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//
//            }
//            pass++;
//
//
//            totalPass = nextRun;
//        }
//
//        //if(pass == 0) pass++;
//
//        try {
//            File orig = new  File(setName(pass, 0));
//            File result = new File(tempout + id + "_result");
//            orig.renameTo(result);
//            reader = new TupleReader(result);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//    }
//
//
//
//
//
//
//
//}

package operators;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import java.util.UUID;

import net.sf.jsqlparser.statement.select.OrderByElement;
import jnio.*;
import util.DBCatalog;
import util.Tuple;

public class ExternalSort extends SortOperator {

    private final String id = UUID.randomUUID().
            toString().substring(0, 8);
    private final String localDir = DBCatalog.tempdir +
            id + File.separator;
    private TupleReader tr = null;
    private List<TupleReader> buffers =
            new ArrayList<TupleReader>(DBCatalog.config.sortPage - 1);
    private int tpsPerPg = 0;

    @Override
    public Tuple getNextTuple() {
        if (tr == null) return null;
        try {
            Tuple tp = tr.read();
            //  if (tp != null) System.out.println(tp.toString());
            return tp;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }


    @Override
    public void reset() {
        if (tr == null) return;
        try {
            tr.reset();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void reset(int idx) {
        if (tr == null) return;
        try {
            tr.reset(idx);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private String fileName(int pass, int run) {
        return localDir + String.valueOf(pass) +
                "_" + String.valueOf(run);
    }

    private int merge(int curPass, int lastRuns) {
        int curRuns = 0;
        int i = 0;

        while (i < lastRuns) {
            buffers.clear();

            int maxJ = Math.min(i + DBCatalog.config.sortPage - 1, lastRuns);
            for (int j = i; j < maxJ; j++) {
                try {
                    TupleReader tr = new TupleReader(new File(
                            fileName(curPass - 1, j)));
                    buffers.add(tr);
                } catch (IOException e) {
                    e.printStackTrace();
                    break;
                }
            }

            try {
                TupleWriter tw = new TupleWriter(fileName(curPass, curRuns++));
                PriorityQueue<Tuple> pq = new PriorityQueue<Tuple>(DBCatalog.config.sortPage - 1,
                        compare);

                for (TupleReader tr : buffers) {
                    Tuple tp = tr.read();
                    if (tp != null) {
                        tp.tupleReader = tr;
                        pq.add(tp);
                    }
                }

                while (!pq.isEmpty()) {
                    Tuple tp = pq.poll();
                    tw.write(tp);
                    TupleReader tr = tp.tupleReader;
                    tp = tr.read();
                    if (tp != null) {
                        tp.tupleReader = tr;
                        pq.add(tp);
                    }
                }

                tw.close();

                for (TupleReader tr : buffers)
                    tr.close();

                for (int j = i; j < maxJ; j++) {
                    File file = new File(fileName(curPass - 1, j));
                    file.delete();
                }
            } catch (IOException e) {
                e.printStackTrace();
                break;
            }

            i += DBCatalog.config.sortPage - 1;
        }

        return curRuns;
    }



    public ExternalSort(Operator child, List<?> orders) {
        super(child, orders);

        new File(localDir).mkdirs();
        tpsPerPg = 4096 / (
                4 * schema.size());
        //the total number of tuples in a run(temp output file)
        int tpsPerRun = tpsPerPg * DBCatalog.config.sortPage;
        //create a array to keep the sorted tuple
        List<Tuple> tps = new ArrayList<Tuple>(tpsPerRun);

        int i = 0;
        while (true) {
            try {
                tps.clear();
                int cnt = tpsPerRun;
                Tuple tp = null;
                while (cnt-- > 0 &&
                        (tp = child.getNextTuple()) != null)
                    tps.add(tp);

                if (tps.isEmpty()) break;

                Collections.sort(tps, compare);

                TupleWriter tw = new TupleWriter(
                        fileName(0, i++));
                for (Tuple tuple : tps)
                    tw.write(tuple);
                tw.close();

                if (tps.size() < tpsPerRun) break;
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }
        }

        if (i == 0) return;

        int curPass = 1;
        int lastRuns = i;
        while (lastRuns > 1)
            lastRuns = merge(curPass++, lastRuns);

        File oldFile = new File(fileName(curPass - 1, 0));
        File newFile = new File(localDir + "final");
        oldFile.renameTo(newFile);

        try {
            tr = new TupleReader(new File(localDir + "final"));
        } catch (IOException e) {
            tr = null;
        }
    }
}




