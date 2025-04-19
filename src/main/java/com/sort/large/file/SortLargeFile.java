package com.sort.large.file;

import com.sort.large.file.util.SortingUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/*
Your task is to write code for a sorting algorithm to sort a file of size 10 GB containing
n numbers with either logarithmic (log 2 N) or linearithmic (N * log 2 N) time complexity. The
constraint is the heap memory available to the java process is only 1024 MB. In other
words you cannot load the entire file in memory at the same time. You should NOT use
any java collections class (any class from java.util).
 */

/*
Approach for sorting:
- reading file
    - splitting into chunks of size 100MB
        - considering each chunk has 10 lines
            - sort line by line using merge sort
                - arrange sorted 2D array using 1st element
            - store back the sorted 2d array into temp chunk file
         - Merge chunks approach
            - Open all buffer readers for temp chunk files.
            - Pick the lowest first element value from line in a chunk and store it in result and increase the readline from that chunk.

Approach for searching:
    - go by first element and last element data
 */

public class SortLargeFile {

    // 100 MB per chunk
    private static final int CHUNK_SIZE = 100 * 1024 * 1024;
    private static final int ELEMENTS_IN_A_ROW = 10;

    public static void main(String[] args) throws IOException {

        String inputFile = "/Users/masundi1/intuit/sort-large-file/src/main/resources/input.txt";
        String outputFile = "/Users/masundi1/intuit/sort-large-file/src/main/resources/output.txt";

        SortLargeFile sortLargeFile = new SortLargeFile();
        sortLargeFile.sortingLargeFile(inputFile, outputFile);
    }

    public void sortingLargeFile(String inputFile, String outputFile) throws IOException {

        // Split and sort chunks
        int numChunks = splitAndSortChunks(inputFile);

        // Merge sorted chunks
        mergeSortedChunks(numChunks, outputFile);
    }


    int splitAndSortChunks(String inputFile) throws IOException {

        int chunkCount = 0;
        ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        List<Future<?>> futures = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(inputFile))) {

            String[] buffer = new String[2];
            int index = 0;
            String line;

            while ((line = reader.readLine()) != null) {
                buffer[index++] = line;

                // buffer is full, sort and write to a temporary file
                if (index == buffer.length) {
                    final int currentChunkIndex = chunkCount++;
                    final String[] currentBuffer = buffer.clone();
                    futures.add(executorService.submit(() -> {
                        try {
                            writeSortedChunk(currentBuffer, currentBuffer.length, currentChunkIndex);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }));
                    index = 0;
                }
            }

            // remaining
            if (index > 0) {
                final int currentChunkIndex = chunkCount++;
                final String[] currentBuffer = buffer.clone();
                int finalIndex = index;
                futures.add(executorService.submit(() -> {
                    try {
                        writeSortedChunk(currentBuffer, finalIndex, currentChunkIndex);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }));
            }
        }

        // Wait for all tasks to complete
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        executorService.shutdown();
        return chunkCount;
    }
    private void writeSortedChunk(String[] numberLines, int size, int chunkIndex) throws IOException {

        convertAndSortLinesByFirstValue(numberLines, size);

        try (BufferedWriter writer = new BufferedWriter(new FileWriter("chunk_" + chunkIndex + ".txt"))) {

            for (int i = 0; i < size; i++) {
                writer.write(numberLines[i]);
                writer.newLine();
            }

        }

    }

    private void convertAndSortLinesByFirstValue(String[] numberLines, int size) {
        int[][] twoDArray = new int[size][ELEMENTS_IN_A_ROW];

        for (int i = 0; i < size; i++) {

            String[] numbers = numberLines[i].split(" ");

            int length = numbers.length;

            twoDArray[i] = stringArrToIntArr(length, numbers);

            // Sort the integer numberLines
            SortingUtils.mergeSort(twoDArray[i], 0, length - 1);
        }

        // sorting by 1st value
        sortByFirstValueIn2D(twoDArray, twoDArray.length);

        for (int i = 0; i < size; i++) {
            StringBuilder sortedString = new StringBuilder();
            for (int j = 0; j < ELEMENTS_IN_A_ROW; j++) {
                if (j > 0) {
                    sortedString.append(" ");
                }
                sortedString.append(twoDArray[i][j]);
            }
            numberLines[i] = sortedString.toString();
        }
    }

    private void sortByFirstValueIn2D(int[][] array, int size) {
        for (int i = 0; i < size - 1; i++) {
            for (int j = 0; j < size - i - 1; j++) {
                if (array[j][0] > array[j + 1][0]) {
                    // Swap the rows
                    int[] temp = array[j];
                    array[j] = array[j + 1];
                    array[j + 1] = temp;
                }
            }
        }
    }

    private int[] stringArrToIntArr(int length, String[] numbers) {
        int[] arr = new int[length];
        for (int j = 0; j < length; j++) {
            arr[j] = Integer.parseInt(numbers[j]);
        }
        return arr;
    }

    private void mergeSortedChunks(int numChunks, String outputFile) throws IOException {
        BufferedReader[] readers = new BufferedReader[numChunks];
        try {
            // Open all chunk files
            for (int i = 0; i < numChunks; i++) {
                readers[i] = new BufferedReader(new FileReader("chunk_" + i + ".txt"));
            }

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {

                String[] topLines = new String[numChunks];
                boolean[] finished = new boolean[numChunks];

                // Initialize top lines from each chunk
                for (int i = 0; i < numChunks; i++) {
                    topLines[i] = readers[i].readLine();
                    if (topLines[i] == null) {
                        finished[i] = true;
                    }
                }

                // Merge process
                mergingChunks(numChunks, finished, topLines, writer, readers);

            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            throw new RuntimeException(e);
        } finally {
            // Close all readers
            for (BufferedReader reader : readers) {
                if (reader != null) {
                    reader.close();
                }
            }
        }

        // Delete temporary chunk files
        for (int i = 0; i < numChunks; i++) {
            new File("chunk_" + i + ".txt").delete();
        }
    }

    private void mergingChunks(int numChunks, boolean[] finished, String[] topLines, BufferedWriter writer, BufferedReader[] readers) throws IOException {

        while (true) {
            int minIndex = -1;
            int[] minArray = null;

            // Find the minimum element among the top lines
            for (int i = 0; i < numChunks; i++) {

                if (!finished[i]) {

                    int[] currentArray = stringArrToIntArr(ELEMENTS_IN_A_ROW, topLines[i].split(" "));

                    if (minArray == null || currentArray[0] < minArray[0]) {
                        minArray = currentArray;
                        minIndex = i;
                    }
                }
            }

            if (minIndex == -1) {
                break;
            }

            writer.write(topLines[minIndex]);
            writer.newLine();

            topLines[minIndex] = readers[minIndex].readLine();
            if (topLines[minIndex] == null) {
                finished[minIndex] = true;
            }
        }
    }
}
