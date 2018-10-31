package jnio;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SortTuple {
    private BufferedReader lines;
    private BufferedWriter sorted;
    private String input;
    private String output;

    public SortTuple(String input, String output) throws IOException {
        this.input = input;
        this.output = output;
        sort();
    }

    private void sort() throws IOException {
        try {
            lines = new BufferedReader(new FileReader(input));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        List<String> listToSort = new ArrayList<>();
        String tuple; while ((tuple = lines.readLine()) != null) {
            listToSort.add(tuple);
        }
        lines.close();
        Collections.sort(listToSort);
        try {
            sorted = new BufferedWriter(new FileWriter(output));
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        for (String str : listToSort) {
            sorted.write(str);
        }
        sorted.close();
    }
}
