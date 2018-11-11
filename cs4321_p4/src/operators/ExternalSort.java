package operators;

import jnio.TupleReader;
import jnio.TupleWriter;
import net.sf.jsqlparser.statement.select.OrderByElement;
import util.Configure;
import util.DBCatalog;
import util.Tuple;

import java.io.*;
import java.util.*;
import java.util.Random.*;

/**
 * External Merge Sort Algoritms sorts file in multi passes
 * and combine final result in "id_result" file
 * @author Yao Xiao
 */
public class ExternalSort extends SortOperator{
    /**
     * tempout: temporary output path
     */
    private String tempout;
    /**
     * uniq id for different externalsort operator
     */
    private int id;
    /**
     * Number of bufferpage in Main memory
     */
    private int bufferPages;

    private Operator child;

    public TupleReader reader = null;


    public ExternalSort(Operator child, List<?> ties) {
        super(child, ties);
        Random random = new Random();
        id = random.nextInt(1000000);
        bufferPages = DBCatalog.config.sortPage - 1;
        this.child = child;


        tempout = DBCatalog.tempdir;

        sort();


    }
    @Override
    public void reset() {
        if (reader == null) return;
        try {
            reader.reset();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    @Override
    public void reset(int index) {
       if(reader == null) return;

        try {
            reader.reset(index);
        }catch (Exception e) {
            e.printStackTrace();
        }


    }




    private String setName(int pass, int run) {
        return tempout + Integer.toString(pass) + "#" + Integer.toString(run) + "_" + Integer.toString(id);
    }

    @Override
    public Tuple getNextTuple() {
        if (reader == null) return null;
       try {
           Tuple tp = reader.read();
          // if(tp != null) System.out.println(tp.toString());
           return tp;
       } catch (IOException e) {
           e.printStackTrace();
       }
       return null;
    }

    @Override
    public String print() {
        return String.format("ExternalSort%s",
                ((sort == null) ? "[]" : sortInEle.toString())) + "\n";
    }

    @Override
    public void printTree(PrintStream ps, int lv) {
        printIndent(ps, lv);
        ps.print(print());
        child.printTree(ps, lv + 1);
    }

    /**
     * sort sep in External Sort Algoritms
     */
    private void sort() {



        int tuplesInPage = 4096 /child.schema.size() / 4;
        int tuplesInRun = tuplesInPage * bufferPages;
        Tuple tp;
        int run = 0;

        while((tp = child.getNextTuple()) != null) {
            List<Tuple> listToSort = new ArrayList<>(tuplesInRun);
            int remain = tuplesInRun;
            while(remain > 0 && tp != null) {
                listToSort.add(tp);

                tp = child.getNextTuple();

                remain--;
            }
            if(tp != null) listToSort.add(tp);
            Collections.sort(listToSort, compare);
            try {
               // System.out.println("Pass 0 file: " + setName(0, run));
                TupleWriter tw = new TupleWriter(setName(0, run));
                for(Tuple t : listToSort) {
                    tw.write(t);

                }
                tw.close();

            } catch (IOException e) {
                e.printStackTrace();
            }

            run++;
        }
        if(run == 0) return;
        merge(run);


    }

    /**
     * Merge sep in External Sort Algoritms
     */
    private void merge(int Pass) {


        int totalPass = Pass;
        int pass = 0;
        int outCount = Pass;


        while(totalPass > 1) {
           // System.out.println("Pass number " + totalPass);
            int nextRun = 0;
            List<TupleReader> buffer = new ArrayList<>();
            int num = Math.min(outCount, bufferPages);

            for(int i = 0; i < totalPass; i+= bufferPages) {
                outCount = i/bufferPages;
                num = Math.min(totalPass - i, bufferPages);
                try {
                    for (int j = i; j < i + num; j++) {
                        buffer.add(new TupleReader(new File(setName(pass, j))));
                    }
                   // System.out.println("output file name is adding file: " + setName(pass + 1, outCount));
                    TupleWriter outputPage = new TupleWriter(setName(pass + 1, outCount));
                    PriorityQueue<Tuple> pq = new PriorityQueue<>(compare);
                    Tuple tuple = null;
                    for (TupleReader tr : buffer) {
                        Tuple tp = tr.read();
                        if (tp != null) {
                            tp.tupleReader = tr;
                            pq.add(tp);

                        }
                    }

                    while (!pq.isEmpty()) {
                        Tuple tp = pq.poll();
                        outputPage.write(tp);
                        //  System.out.println(tp.toString());
                        TupleReader tr = tp.tupleReader;
                        tp = tr.read();
                        if (tp != null) {
                            tp.tupleReader = tr;
                            pq.add(tp);
                        }
                    }
                    for (int j = i; j < i + num; j++) {
                        File file = new File(setName(pass , j));
                    //    System.out.println("detleting " + setName(pass, j));
                        file.delete();
                    }

                    buffer.clear();
                    outputPage.close();
                    nextRun++;

                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
            pass++;


            totalPass = nextRun;
        }
        try {
            File orig = new  File(setName(pass, 0));
            File result = new File(tempout + id + "_result");
            orig.renameTo(result);
            reader = new TupleReader(result);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}





