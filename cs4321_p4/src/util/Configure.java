package util;

import jnio.TupleReader;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class Configure {
    private File config;
    public int TNLJ = 0;
    public int BNLJ = 0;
    public int SMJ = 0;
    public int inMemSort = 0;
    public int externalSort = 0;
    public int sortPage = 0;
    public int joinPage = 0;
    public boolean idxSelect = false;
    private BufferedReader tr = null;
    public Configure(String path) {
        config = new File(path);

        setUp();
       // manualSetUp();
    }
    private void manualSetUp(){

    }
    private void setUp() {
        try{
            tr = new BufferedReader(new FileReader(config));
            String[] num = new String[2];
            for(int i = 0; i < 3; i++) {
                String tp = tr.readLine();
                num = tp.split(" ");
                if(i == 0) {
                   switch (num[0]) {
                       case "0": TNLJ = 1;
                        continue;
                       case "1": BNLJ = 1;
                       joinPage = Integer.valueOf(num[1]);
                       continue;
                       case "2": SMJ = 1;

                   }
                } else if (i == 1) {
                    switch (num[0]) {
                        case "0": inMemSort = 1;
                        continue;
                        case "1": externalSort = 1;
                            sortPage = Integer.valueOf(num[1]);


                    }
                } else {
                    idxSelect = num[0].equals("1");
                }
            }
            tr.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
