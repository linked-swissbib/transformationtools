package org.gesis.splitfile;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;

/**
 * This program separates line based files into several smaller files Date
 */
public class App {

    public static final String USAGE = "Error. Usage: splitter <infile> <outdir> <lines per file> <file extension>";
    public static final String ENCODING = "UTF-8";

    /**
     * This is the entry point. Also, this is the only class and method in this
     * program.
     *
     * @param args
     */
    public static void main(String[] args) {

        System.out.println("Platform file encoding: " + System.getProperty("file.encoding"));
        System.out.println("Default charset: " + Charset.defaultCharset().name());

        //check inputs
        if (args.length != 4) {
            System.err.println(USAGE);
            System.exit(-1);
        }
        File inFile = new File(args[0]);
        if (!inFile.exists() || !inFile.isFile()) {
            System.err.println("File does not exist or file is not a file.");
            System.err.println(USAGE);
            System.exit(-1);
        }
        File outDir = new File(args[1]);
        if (!outDir.exists() || !outDir.isDirectory()) {
            System.err.println("Output directory does not exist or is not a directory.");
            System.err.println(USAGE);
            System.exit(-1);
        }

        long linesPerFile = Long.MAX_VALUE;
        try {
            linesPerFile = Long.valueOf(args[2]);
        } catch (NumberFormatException nfe) {
            System.err.println("Arguement three is not a valid number.");
            System.err.println(USAGE);
            System.exit(-1);
        }

        String fileExtension = args[3];
        if (!fileExtension.startsWith(".")) {
            fileExtension = "." + fileExtension;
        }

        try {

            //configure reading and writing
            FileInputStream fis = new FileInputStream(inFile);
            InputStreamReader isr = new InputStreamReader(fis, ENCODING);
            BufferedReader br = new BufferedReader(isr);

            long fileCnt = 0;  //counts created files
            File curOutFile = new File(outDir, String.format("%03d", fileCnt) + fileExtension);

            FileOutputStream fos = new FileOutputStream(curOutFile);
            OutputStreamWriter osw = new OutputStreamWriter(fos, ENCODING);
            BufferedWriter bw = new BufferedWriter(osw);

            long cnt = 1;  //counts accepted lines in current output file
            long acceptedLines = 0;  //counts lines that are copied
            long droppedLines = 0;  //counts lines that are not copied
            long totalLines = 0;   //counts accepted and dropped lines

            //begin
            String line = br.readLine();

            while (line != null) { //as long as there are lines do...
                totalLines++;
                //check current line
                line = replaceUnwantedSequence(line);
                if (line==null) {
                    droppedLines++;
                } else {
                    //append line
                    cnt++;
                    acceptedLines++;

                    if (cnt <= linesPerFile) { //write line into current file
                        bw.write(line);
                        bw.newLine();
                    }
                    if (cnt == linesPerFile) { //if file full open new file
                        bw.close();
                        curOutFile = new File(outDir, String.format("%03d", fileCnt) + fileExtension);
                        fos = new FileOutputStream(curOutFile);
                        osw = new OutputStreamWriter(fos, ENCODING);
                        bw = new BufferedWriter(osw);
                        fileCnt++;
                        System.out.println("[+] Created new file " + curOutFile.getAbsolutePath());
                        cnt = 0;
                    }
                }
                line = br.readLine();
            }
            bw.close(); //close all files
            br.close();

            //print statistic
            System.out.println("Done.");
            System.out.println("Total lines " + totalLines);
            System.out.println("Accepted lines " + acceptedLines);
            System.out.println("Dropped lines " + droppedLines);
            System.out.println("Total files " + fileCnt);
            return; //successful finished
        } catch (IOException ioe) {
            System.err.println(ioe.getMessage());
            System.exit(-1);
        }

    }

    /**
     * Searches in a String for surrogates
     *
     * @param s The String to examine.
     * @return true if a surrogate character is present, false otherwise.
     */
    private static boolean containsUnwantedCharacter(String s) {
        for (int i = 0; i < s.length();) {
            int cp = s.codePointAt(i);
            if (cp >= 0xD800) {
                return true;
            }
            i += Character.charCount(cp);
        }
        return false;

    }

    /**
     * Searches in a String for surrogate sequences
     * \uD800 - \uFFFF in plain text these are not valid in UTF-8 (only in UTF-16)
     * Source: https://de.wikipedia.org/wiki/UTF-8
     * 
     * @param s The String to examine.
     * @return true if a surrogate character is present, false otherwise.
     */
    private static boolean containsUnwantedSequence(String s) {

        int idx = 0;
        idx = s.indexOf("\\u", idx);
        while (idx != -1) {
            String subsequ = s.substring(idx + 2, idx + 6);
            int hex = 0;
            try{
                hex = Integer.valueOf(subsequ, 16);
            }catch(NumberFormatException nfe){
                return true;
            }
            if (hex >= 0xD800 && hex <=0xFFFF) {
                return true;
            }
            idx = s.indexOf("\\u", idx + 7);
        }
        return false;

    }
    
    
    /**
     * Searches in a String for surrogate sequences and replaces the surrogates with '?'
     * \uD800 - \uFFFF in plain text these are not valid in UTF-8 (only in UTF-16)
     * Source: https://de.wikipedia.org/wiki/UTF-8
     * 
     * @param s The String to examine.
     * @return true if a surrogate character is present, false otherwise.
     */
    private static String replaceUnwantedSequence(String s) {

        String str = s;
        int idx = 0;
        idx = str.indexOf("\\u", idx);
        while (idx != -1) {
            String subsequ = str.substring(idx + 2, idx + 6);
            int hex = 0;
            try{
                hex = Integer.valueOf(subsequ, 16);
            }catch(NumberFormatException nfe){
                return null;
            }
            if (hex >= 0xD800 && hex <=0xFFFF) {
                str=str.replace("\\u"+subsequ, "?");
            }
            idx = str.indexOf("\\u", idx+1);
        }
        return str;

    }

}
