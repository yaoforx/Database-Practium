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
public class ExternalSort extends Operator{
    private String tempout;
    private int id;
    private TupleReader tr=null;
    private int bufferPages = 0;
    private Operator child;
    List<Integer> sort = new ArrayList<>();
    externalCmp cmp;
    public TupleReader reader = null;

    public class externalCmp implements Comparator<Tuple> {

        @Override
        public int compare(Tuple t1, Tuple t2) {
            HashSet<Integer> orderby = new HashSet<>(sort);
            //  System.out.println(sort.toString());

            for(int i = 0; i < sort.size(); i++) {
                int v1 = t1.getValue(sort.get(i));
                int v2 = t2.getValue(sort.get(i));
                int comp = Integer.compare(v1,v2);
                if(comp !=0) return comp;
            }
            //If you are here means ORDER BY has not broken ties
            //then sort based on original remaining schemas
            for(int i = 0; i < t1.getSize(); i++) {
                if(orderby.contains(i)) continue;
                int v1 = t1.getValue(i);
                int v2 = t2.getValue(i);
                int comp = Integer.compare(v1,v2);
                if(comp !=0) return comp;
            }
            return 0;
        }
        public externalCmp(List<OrderByElement> ties) {
            if(!ties.isEmpty()) {
                for (OrderByElement element : ties) {

                    sort.add(SortOperator.getColIdx(element, child.schema));

                }
            }
        }
    }
    public ExternalSort(Operator child, List<OrderByElement> ties) {
        Random random = new Random();
        id = random.nextInt(10000000);
        bufferPages = DBCatalog.config.sortPage - 1;
        this.child = child;
        cmp = new externalCmp(ties);
        tempout = DBCatalog.tempdir;

        sort();


    }
    @Override
    public void reset() {

    }
    private String setName(int pass, int run) {
        return tempout + Integer.toString(pass) + "#" + Integer.toString(run) + "_" + Integer.toString(id);
    }
    @Override
    public Tuple getNextTuple() {

       try {
           Tuple tp = reader.read();
           System.out.println(tp.toString());
           return tp;
       } catch (IOException e) {
           e.printStackTrace();
       }
       return null;
    }
    private void sort() {



        int tuplesInPage = 4096 /child.schema.size() / 4;
        int tuplesInRun = tuplesInPage * bufferPages;
        Tuple tp;
        int run = 0;
        while((tp = child.getNextTuple()) != null) {
            List<Tuple> listToSort = new ArrayList<>();
            int remain = tuplesInRun;
            while(remain > 0 && tp != null) {
                listToSort.add(tp);

                tp = child.getNextTuple();
                remain--;
            }
            Collections.sort(listToSort, cmp);
            try {
                System.out.println("Pass 0 file: " + setName(0, run));

                TupleWriter tw = new TupleWriter(setName(0, run));
                for(Tuple t : listToSort) tw.write(t);

            } catch (IOException e) {
                e.printStackTrace();
            }

            run++;
        }
        merge(run - 1);


    }
    private void merge(int Pass) {


        int totalPass = Pass;
        int pass = 0;


        while(totalPass > 1) {
            System.out.println("Pass num ber " + totalPass);
            int outCount = Integer.MAX_VALUE;

            List<TupleReader> buffer = new ArrayList<>();


            for(int i = 0; i < totalPass; i+= bufferPages) {

                int num = Math.min(outCount, bufferPages);

                try {
                    for (int j = 0; j < num; j++) {

                        System.out.println("num is " + num + "Input file name is adding file: " + setName(pass, j));

                        buffer.add(new TupleReader(new File(setName(pass, j))));
                    }
                    pass++;
                    outCount = i/bufferPages;
                    System.out.println("output file name is adding file: " + setName(pass, outCount));
                    TupleWriter outputPage = new TupleWriter(setName(pass, outCount++));
                    PriorityQueue<Tuple> pq = new PriorityQueue<>(cmp);
                    Tuple tuple = null;

                    while(true) {
                        while(!pq.isEmpty()) {
                            Tuple tp = pq.poll();
                            if (tp != null) outputPage.write(tp);
                        }
                        for(int k = 0; k < buffer.size(); k++) {
                            tuple = buffer.get(k).read();
                            if(tuple == null) {
                                buffer.get(k).close();
                                buffer.remove(buffer.get(k));

                                continue;
                            }
                            pq.add(tuple);
                        }
                        if(buffer.size() == 0) break;
                    }
                    for (int j = 0; j < num; j++) {
                       // File file = new File(setName(pass, j));
                       // System.out.println("detleting " + setName(pass - 1, j));
                       // file.delete();
                    }
                    buffer.clear();
                    outputPage.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }



            }
            totalPass = totalPass/outCount;

        }
        try {
            reader = new TupleReader(new File(setName(pass - 1, 0)));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }




}
