/**
 * Created by RyanW on 4/24/16.
 */
import org.apache.commons.lang.RandomStringUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class GenerateRandomTextFile {
    private static final int ITERATIONS = 1;
    private static final double MEG = (Math.pow(1024, 2));
    private static final int RECORD_COUNT = 4000000;
    private static final int RECSIZE = 10;

    public static void main(String[] args) throws Exception {
        List<String> records = new ArrayList<String>(RECORD_COUNT);
        int size = 0;
        for (int i = 0; i < RECORD_COUNT; i++) {
            String line = RandomStringUtils.randomAlphanumeric(RECSIZE) + "\n";
            records.add(line);
            size += line.getBytes().length;
        }
        System.out.println(records.size() + " 'records'");
        System.out.println(size / MEG + " MB");

        for (int i = 0; i < ITERATIONS; i++) {
            System.out.println("\nIteration " + i);

            writeBuffered(records, 4 * (int) MEG);
        }
    }

    private static void writeBuffered(List<String> records, int bufSize) throws IOException {
        File dir = new File("data/");
        File file = new File(dir, "data.txt");
        try {
            FileWriter writer = new FileWriter(file);
            BufferedWriter bufferedWriter = new BufferedWriter(writer, bufSize);

            System.out.print("Writing buffered (buffer size: " + bufSize + ")... ");
            write(records, bufferedWriter);
        } finally {
            System.out.print("Output File Path: " + file.getAbsolutePath());
        }
    }

    private static void write(List<String> records, Writer writer) throws IOException {
        long start = System.currentTimeMillis();
        for (String record: records) {
            writer.write(record);
        }
        writer.flush();
        writer.close();
        long end = System.currentTimeMillis();
        System.out.println((end - start) / 1000f + " seconds");
    }
}
