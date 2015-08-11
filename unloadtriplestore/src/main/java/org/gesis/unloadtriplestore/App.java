package org.gesis.unloadtriplestore;

import java.io.OutputStream;
import java.util.concurrent.LinkedBlockingQueue;
import org.openrdf.OpenRDFException;
import org.openrdf.model.Statement;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sparql.SPARQLRepository;

/**
 * Requests every statement from a SPARQL endpoint and prints it to System.out
 * as NTriples.
 *
 */
public class App {

    private static final String USAGE = "Usage: <sparql ep>";
    private static final int THREADS = 3;
    private static final int PAGESIZE = 1000;
    private static final int QCAPACITY = 200000;

    public static void main(String[] args) {

        //check input
        if (args.length != 1) {
            System.out.println(USAGE);
            return;
        }
        //check sparql
        String sparqlEp = args[0];
        Repository repo = new SPARQLRepository(sparqlEp);
        try {
            repo.initialize();
            RepositoryConnection con = repo.getConnection();
            con.close();
        } catch (OpenRDFException e) {
            System.err.println("Unable to connect to: " + sparqlEp + " " + e.getMessage());
            return;
        }

        //the unlaoding
        try {
            unload(sparqlEp, System.out);
        } catch (RepositoryException ex) {
            System.err.println(ex);
        }

    }

    
    /**
     * Starts threads to unload the tripplestore behind the SPARQL endpoint.
     * @param sparqlEp
     * @param outStream
     * @throws RepositoryException 
     */
    private static void unload(String sparqlEp, OutputStream outStream) throws RepositoryException {
        LinkedBlockingQueue<Statement> queue = new LinkedBlockingQueue<Statement>(QCAPACITY);
        for (int i = 0; i < THREADS; i++) {
            SPARQLRequestor req = new SPARQLRequestor(sparqlEp, queue);
            req.setOffset(i * PAGESIZE);
            req.setLimit(PAGESIZE);
            req.setSkip(THREADS * PAGESIZE);
            Thread thread = new Thread(req, "Requestor " + i);
            thread.start();
        }
        NTripleWriter ntWriter = new NTripleWriter(queue, outStream);
        Thread thread = new Thread(ntWriter);
        thread.start();
    }
}
