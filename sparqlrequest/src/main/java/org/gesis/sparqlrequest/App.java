package org.gesis.sparqlrequest;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import org.openrdf.OpenRDFException;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sparql.SPARQLRepository;

/**
 * Requests resources from a SPARQL endpoint and writes them to System.out as
 * NTriples. Resources can be specified by a list of resource IDs. This list can
 * be passed as parameter or via bash piping.
 *
 */
public class App {

    private static final String USAGE = "Usage: sparqlrequest <input file> <endpoint>\n";
    private static final String ENCODING = "UTF-8";

    public static void main(String[] args) {
        String sparqlEp = null;
        InputStream inStream = null;

        if (args.length < 1 || args.length > 2) {
            System.err.println(USAGE);
            return;
        }
        if (args.length == 1) {
            inStream = System.in;
            sparqlEp = args[0];
        } else {
            sparqlEp = args[1];
            File file = new File(args[0]);
            if (!file.exists() || !file.isFile()) {
                System.err.println("No such file:" + file.getAbsolutePath());
                return;
            }
            try {
                inStream = new FileInputStream(file);
            } catch (FileNotFoundException ex) {
                System.err.println(ex.getMessage());
                return;
            }
        }

        Repository repo = new SPARQLRepository(sparqlEp);
        try {
            repo.initialize();
            RepositoryConnection con = repo.getConnection();
            con.close();
        } catch (OpenRDFException e) {
            System.err.println("Unable to connect to: " + sparqlEp + " " + e.getMessage());
            return;
        }

        try {
            if (inStream.available() <= 0) {
                return;
            }
        } catch (IOException ex) {
            System.err.println(ex.getMessage());
            return;
        }

        try {
            processResourceList(inStream, System.out, sparqlEp);
        } catch (UnsupportedEncodingException ex) {
            System.err.println(ex);
        } catch (IOException ex) {
            System.err.println(ex);
        } catch (RepositoryException ex) {
            System.err.println(ex);
        } catch (MalformedQueryException ex) {
            System.err.println(ex);
        } catch (QueryEvaluationException ex) {
            System.err.println(ex);
        }
        try {
            inStream.close();
        } catch (IOException ex) {
            System.err.println("Unable to close input stream.");
        }
        System.out.close();
        return;

    }

    private static void processResourceList(InputStream inStream, OutputStream outStream, String sparqlEp) throws UnsupportedEncodingException, IOException, RepositoryException, MalformedQueryException, QueryEvaluationException {
        Repository repo = new SPARQLRepository(sparqlEp);
        repo.initialize();
        RepositoryConnection con = repo.getConnection();

        BufferedReader bReader = new BufferedReader(new InputStreamReader(inStream, ENCODING));
        BufferedWriter bWriter = new BufferedWriter(new OutputStreamWriter(outStream, ENCODING));

        String line = bReader.readLine();

        while (line != null) {
            String request = "SELECT ?p ?o WHERE { <" + line + "> ?p ?o }";
            TupleQuery query = con.prepareTupleQuery(QueryLanguage.SPARQL, request);
            query.setIncludeInferred(false);
            TupleQueryResult result = query.evaluate();
            while (result.hasNext()) {
                BindingSet bindingSet = result.next();
                Value predicate = bindingSet.getValue("p");
                Value object = bindingSet.getValue("o");
                bWriter.write("<" + line + "> ");
                bWriter.write("<" + predicate.stringValue() + "> ");
                if (object instanceof org.openrdf.model.Literal) {
                    bWriter.write("\"" + object.stringValue() + "\" ");
                } else {
                    bWriter.write("<" + object.stringValue() + ">");
                }
                bWriter.write(".");
                bWriter.newLine();
                bWriter.flush();
            }
            line = bReader.readLine();
        }

    }

}
