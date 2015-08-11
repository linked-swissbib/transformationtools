package org.gesis.unloadtriplestore;

import java.util.concurrent.LinkedBlockingQueue;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
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
 * Request statements in the given range from a SPARQL endpoint. It is designed
 * to cooperate with other SPARQLRequestors.
 *
 */
public class SPARQLRequestor implements Runnable {

    private RepositoryConnection con = null;
    private LinkedBlockingQueue<Statement> queue = null;
    private long offset = 0;
    private long limit = -1;
    private long skip = 0;
    private Exception lastException = null;

    public SPARQLRequestor(String sparqlEp, LinkedBlockingQueue<Statement> q) throws RepositoryException {
        this.queue = q;
        Repository repo = new SPARQLRepository(sparqlEp);
        repo.initialize();
        con = repo.getConnection();
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public long getLimit() {
        return limit;
    }

    public void setLimit(long limit) {
        this.limit = limit;
    }

    public long getSkip() {
        return skip;
    }

    public void setSkip(long skip) {
        this.skip = skip;
    }

    public Exception getLastException() {
        return lastException;
    }
    
    

    @Override
    public void run() {
        int cnt = 1;
        try {
            while (cnt > 0) {
                String request = "SELECT ?s ?p ?o WHERE { ?s ?p ?o } OFFSET " + offset + " LIMIT " + limit;

                cnt = 0;
                TupleQuery query = con.prepareTupleQuery(QueryLanguage.SPARQL, request);
                query.setIncludeInferred(false);
                TupleQueryResult result = query.evaluate();
                while (result.hasNext()) {
                    cnt++;
                    BindingSet bindingSet = result.next();
                    Value subject = bindingSet.getValue("s");
                    Value predicate = bindingSet.getValue("p");
                    Value object = bindingSet.getValue("o");
                    Statement stmt = new StatementImpl(new URIImpl(subject.stringValue()), new URIImpl(predicate.stringValue()), object);
                    queue.put(stmt);
                }
                offset += skip;
            }
        } catch (QueryEvaluationException ex) {
            lastException = ex;
        } catch (RepositoryException ex) {
            lastException = ex;
        } catch (MalformedQueryException ex) {
            lastException = ex;
        } catch (InterruptedException ex) {
            lastException = ex;
        } finally {
            try {
                con.close();
            } catch (RepositoryException ex) {
                lastException = ex;
            }
        }
    }

}
