package org.gesis.unloadtriplestore;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;

/**
 * Implementation of Runnable, reads statements from a queue and writes them to
 * System.out as NTriples.
 *
 */
public class NTripleWriter implements Runnable {

    private LinkedBlockingQueue<Statement> queue = null;
    private OutputStream outStream = null;
    private static final String ENCODING = "UTF-8";
    private Exception lastException = null;

    public NTripleWriter(LinkedBlockingQueue<Statement> q, OutputStream outStream) {
        this.queue = q;
        this.outStream = outStream;
    }

    public Exception getLastException() {
        return lastException;
    }

    @Override
    public void run() {
        try {
            BufferedWriter bWriter = new BufferedWriter(new OutputStreamWriter(outStream, ENCODING));
            Statement stmt = queue.poll(10, TimeUnit.SECONDS);
            while (stmt != null) {
                bWriter.write("<" + stmt.getSubject().stringValue() + "> ");
                bWriter.write("<" + stmt.getPredicate().stringValue() + "> ");
                Value object = stmt.getObject();
                if (object instanceof org.openrdf.model.Literal) {
                    bWriter.write("\"" + object.stringValue() + "\" ");
                } else {
                    bWriter.write("<" + object.stringValue() + "> ");
                }
                bWriter.write(".");
                bWriter.newLine();
                bWriter.flush();

                stmt = queue.poll(10, TimeUnit.SECONDS);
            }

        } catch (UnsupportedEncodingException ex) {
            lastException = ex;
        } catch (InterruptedException ex) {
            Logger.getLogger(NTripleWriter.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(NTripleWriter.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

}
