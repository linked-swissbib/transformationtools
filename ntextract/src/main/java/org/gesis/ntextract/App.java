package org.gesis.ntextract;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.Rio;

/**
 * CLI tool to extract object URIs from NTriple files. A file can be passed as
 * parameter or via bash pipelining.
 *
 */
public class App {

    private static final String USAGE = "Usage: ntextract <infile> alternatively use piping.";
    private static final String ENCODING = "UTF-8";

    /**
     * Main entry point.
     *
     * @param args
     */
    public static void main(String[] args) {

        //local vars
        File inFile = null;
        InputStream inStream = null;
        BufferedWriter bWriter = null;

        //input check
        if (args.length > 1) {
            System.err.println(USAGE);
            return;
        } //use infile arg
        else if (args.length == 1) {
            inFile = new File(args[0]);
            //check file
            if (!inFile.isFile() || !inFile.exists()) {
                System.err.println("File not found.");
            }
            try {
                inStream = new FileInputStream(inFile);
            } catch (FileNotFoundException ex) {
                System.err.println(ex.getMessage());
                return;
            }
        } //use System.in
        else {
            inStream = System.in;
        }

        try {
            //configure writer
            bWriter = new BufferedWriter(new OutputStreamWriter(System.out, ENCODING));
        } catch (UnsupportedEncodingException ex) {
            System.err.println(ex.getMessage());
            return;
        }

        RDFParser rdfParser = Rio.createParser(RDFFormat.NTRIPLES);
        rdfParser.setRDFHandler(new RDFWriterHandler(bWriter));

        try {
            if (inStream.available() <= 0) {
                return;
            }
            rdfParser.parse(inStream, "");
            bWriter.close();
        } catch (IOException ex) {
            System.err.println(ex.getMessage());
        } catch (RDFParseException ex) {
            System.err.println(ex.getMessage());
        } catch (RDFHandlerException ex) {
            System.err.println(ex.getMessage());
        }

    }

}
