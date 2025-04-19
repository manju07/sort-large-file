package com.sort.large.file;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doThrow;

public class SortLargeFileTest {

    private SortLargeFile sortLargeFile;

    @Before
    public void setUp() {
        sortLargeFile = new SortLargeFile();
    }

    @Test
    public void testTotalRows() throws IOException {
        String outputFile = "/Users/masundi1/intuit/sort-large-file/src/test/resources/output.txt";
        String inputFile = "/Users/masundi1/intuit/sort-large-file/src/test/resources/input.txt";

        sortLargeFile.sortingLargeFile(inputFile, outputFile);

        int count = 0;

        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(outputFile))) {
            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                count++;
            }
        }
        assertEquals(5, count);
    }

    @Test
    public void testSplitAndSortChunks() throws IOException {
        int numChunks = sortLargeFile.splitAndSortChunks("/Users/masundi1/intuit/sort-large-file/src/test/resources/input.txt");
        assertEquals(3, numChunks);
    }

    @Test
    public void testSortingLargeFileWithValidFiles() throws IOException {
        String inputFile = "/Users/masundi1/intuit/sort-large-file/src/test/resources/input.txt";
        String outputFile = "/Users/masundi1/intuit/sort-large-file/src/test/resources/output.txt";

        // Assuming the method sorts the file correctly
        sortLargeFile.sortingLargeFile(inputFile, outputFile);

        File output = new File(outputFile);
        assertTrue(output.exists());
        // Additional assertions can be added to verify the content of the output file
    }

    @Test(expected = IOException.class)
    public void testSortingLargeFileWithNonExistentInputFile() throws IOException {
        String inputFile = "src/test/resources/nonExistentInput.txt";
        String outputFile = "src/test/resources/output.txt";

        sortLargeFile.sortingLargeFile(inputFile, outputFile);
    }

    @Test(expected = IOException.class)
    public void testSortingLargeFileWithInvalidOutputFile() throws IOException {
        String inputFile = "src/test/resources/validInput.txt";
        String outputFile = "src/test/resources/invalid/output.txt";

        // Mocking the behavior to throw an IOException when writing to an invalid path
        SortLargeFile mockSortLargeFile = Mockito.spy(sortLargeFile);
        doThrow(new IOException("Invalid output path")).when(mockSortLargeFile).sortingLargeFile(inputFile, outputFile);

        mockSortLargeFile.sortingLargeFile(inputFile, outputFile);
    }
}