package util;

import java.util.*;
import java.io.*;
import jnio.*;
import org.junit.Test;

public class RandomGenerator {
    private int max;
    private int cols;
    private int numTuples;
    private Random random;
    private TupleWriter writer;

    /**
     * RandomGenerator is used to generate tuples of random numbers for testing
     *
     * @param max       generate random numbers between 1 and max
     * @param cols      number of columns per tuple to be generated
     * @param numTuples total number of tuples to be generated
     * @param path      path to the file where the generated tuples will be written
     */
    public RandomGenerator(int max, int cols, int numTuples, String path) {
        this.max = max;
        this.cols = cols;
        this.numTuples = numTuples;
        this.random = new Random();
        try {
            this.writer = new TupleWriter(path);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * generates the random tuples and writes them to the file
     */
    public void generate() {
        for (int i = 0; i < numTuples; i++) {
            ArrayList<Integer> tempList = new ArrayList<Integer>();
            for (int j = 0; j < cols; j++) {
                int rand = 1 + random.nextInt(max - 1);
                tempList.add(rand);
            }
            Tuple tuple = new Tuple(tempList);
            try {
                writer.write(tuple);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

