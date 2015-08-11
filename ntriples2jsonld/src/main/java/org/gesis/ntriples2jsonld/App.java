package org.gesis.ntriples2jsonld;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.Rio;

/**
 * Converts NTriples files to JSONLD files. Files can be passed as argument or
 * via bash piping.
 *
 */
public class App {

    private static final String USAGE = "Usage: ntriples2jsonld <input file>";
    private static final String ENCODING = "UTF-8";

    public static void main(String[] args) {

        InputStream inStream = null;

        if (args.length > 1) {
            System.err.println(USAGE);
            return;
        }
        if (args.length == 0) {
            inStream = System.in;
        } else {
            File inputFile = new File(args[0]);
            try {
                inStream = new FileInputStream(inputFile);
            } catch (FileNotFoundException ex) {
                System.err.println("File " + inputFile.getAbsolutePath() + "not found.");
                return;
            }
        }

        // create a parser for Turtle and a writer for RDF/XML 
        RDFParser rdfParser = Rio.createParser(RDFFormat.NTRIPLES);
        RDFWriter rdfWriter = Rio.createWriter(RDFFormat.JSONLD, System.out);

        // link our parser to our writer...
        rdfParser.setRDFHandler(rdfWriter);

        // ...and start the conversion!
        try {
            rdfParser.parse(inStream, "");
        } catch (IOException e) {
            // handle IO problems (e.g. the file could not be read)
        } catch (RDFParseException e) {
            // handle unrecoverable parse error
        } catch (RDFHandlerException e) {
            // handle a problem encountered by the RDFHandler
        }

    }
}
