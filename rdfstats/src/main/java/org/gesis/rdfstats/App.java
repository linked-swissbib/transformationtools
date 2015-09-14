package org.gesis.rdfstats;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import static java.lang.Math.pow;
import java.util.ArrayList;
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
 * Tool to request the combinations of fields set for a certain resource type.
 * Outputs are written to a file as well as stdout.
 */
public class App {

    private static final String USAGE = "Usage: rdfstats <sparql endpoint> <resource type> [<property list file>]";

    public static void main(String[] args) {
        //org.apache.log4j.BasicConfigurator.configure();

        String sparqlEp = null;
        String type = null;
        String fileName = null;

        //check args
        if (args.length < 2 || args.length > 3) {
            System.out.println(USAGE);
            return;
        }

        sparqlEp = args[0];
        type = args[1];

        //List to contain the field/properties to query
        ArrayList<String> propertyList = null;

        //In case a file is used
        if (args.length == 3) {
            fileName = args[2];
            File file = new File(fileName);
            if (!file.exists() || !file.isFile()) {
                System.out.println("File " + file.getAbsolutePath() + " is invalid");
                return;
            }
            try {
                propertyList = new ArrayList<String>();
                BufferedReader br = new BufferedReader(new FileReader(file));
                String line = br.readLine();
                while (line != null) {
                    propertyList.add(line);
                    line = br.readLine();
                }
                br.close();
            } catch (FileNotFoundException ex) {
                System.err.println(ex);
                System.exit(-1);
            } catch (IOException ex) {
                System.err.println(ex);
                System.exit(-1);
            }
        } //in case all available properties are to be queried
        else {
            try {
                propertyList = getPropertyNames(sparqlEp, type);
                propertyList.remove("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
            } catch (RepositoryException ex) {
                System.err.println(ex);
                System.exit(-1);
            } catch (MalformedQueryException ex) {
                System.err.println(ex);
                System.exit(-1);
            } catch (QueryEvaluationException ex) {
                System.err.println(ex);
                System.exit(-1);
            }
        }
        //Property output for convenience
        System.out.println("Properties:");
        for (String str : propertyList) {
            System.out.println(str);
        }

        //create a truthtable like table to store the values
        int xDim = propertyList.size() + 1;
        int yDim = (int) pow(2, propertyList.size());
        int[][] table = new int[xDim][yDim];
        for (int y = 0; y < yDim; y++) {
            for (int x = 0; x < xDim - 1; x++) {
                table[xDim - x - 1 - 1][y] = getBit(y, x);
            }
        }

        //construct an issue queries
        for (int y = 0; y < yDim; y++) {
            System.out.println();
            System.out.println("Query #" + (y + 1));
            String query = buildQuery(propertyList, table, y, type);
            System.out.println(query);
            int c = -1;

            try {
                c = getCount(query, sparqlEp);
                System.out.println("Server returned " + c);
            } catch (Exception ex) {
                System.out.println("ERR: " + ex);
            }
            table[table.length - 1][y] = c;
        }

        //write results
        writeToSystemOut(propertyList, table);
        try {
            writeToCSV(new File("out.csv"), propertyList, table);
        } catch (FileNotFoundException ex) {
            System.err.println(ex);
            System.exit(-1);
        }

        System.exit(0);
    }

    /**
     * Requests all properties for a certain type
     *
     * @param sparqlEp The endpoint to send the request to
     * @param type The to query (no shorteners)
     * @return A list of all distinct properties
     * @throws RepositoryException
     * @throws MalformedQueryException
     * @throws QueryEvaluationException
     */
    private static ArrayList<String> getPropertyNames(String sparqlEp, String type) throws RepositoryException, MalformedQueryException, QueryEvaluationException {

        Repository repo = new SPARQLRepository(sparqlEp);
        RepositoryConnection con = null;

        repo.initialize();
        con = repo.getConnection();

        String request = "SELECT DISTINCT ?p WHERE { ?s a <" + type + "> . ?s ?p ?o }";
        TupleQuery query = con.prepareTupleQuery(QueryLanguage.SPARQL, request);
        query.setIncludeInferred(false);
        TupleQueryResult result = query.evaluate();
        ArrayList<String> predicateList = new ArrayList<String>();
        while (result.hasNext()) {
            BindingSet bindingSet = result.next();
            Value predicate = bindingSet.getValue("p");
            predicateList.add(predicate.stringValue());
        }
        result.close();
        con.close();
        return predicateList;
    }

    /**
     * Returns the specified bit of an integer
     *
     * @param n the integer
     * @param k index of the bit
     * @return
     */
    private static int getBit(int n, int k) {
        return (n >> k) & 1;
    }

    /**
     * Constructs a SPARQL query for a certain field combination
     *
     * @param propertyList Reference list for properties
     * @param table Reference table for actual combination
     * @param row Row to use from table
     * @param type Type to request from
     * @return The constructed row
     */
    private static String buildQuery(ArrayList<String> propertyList, int[][] table, int row, String type) {
        String query = "SELECT COUNT( DISTINCT ?s) AS ?count\nWHERE {\n";
        query += "  ?s rdf:type <" + type + "> . \n";
        for (int col = 0; col < propertyList.size(); col++) {
            if (table[col][row] == 1) {
                query += "  ?s <" + propertyList.get(col) + "> ?v" + col + " . \n";
            }
        }
        for (int col = 0; col < propertyList.size(); col++) {
            if (table[col][row] == 0) {
                query += "  filter not exists { ?s <" + propertyList.get(col) + "> ?v" + col + " }\n";
            }
        }
        query += "}";
        return query;
    }

    /**
     * Issues the query and returns the result.
     *
     * @param request
     * @param sparqlEp
     * @return
     * @throws RepositoryException
     * @throws MalformedQueryException
     * @throws QueryEvaluationException
     */
    private static int getCount(String request, String sparqlEp) throws RepositoryException, MalformedQueryException, QueryEvaluationException {

        Repository repo = new SPARQLRepository(sparqlEp);
        RepositoryConnection con = null;
        repo.initialize();
        con = repo.getConnection();

        TupleQuery query = con.prepareTupleQuery(QueryLanguage.SPARQL, request);
        query.setIncludeInferred(false);
        TupleQueryResult result = query.evaluate();

        BindingSet bindingSet = result.next();
        Value count = bindingSet.getValue("count");
        result.close();

        con.close();

        return Integer.valueOf(count.stringValue());

    }

    /**
     * Writes results to System.out as a table
     *
     * @param predicateList
     * @param table
     */
    private static void writeToSystemOut(ArrayList<String> predicateList, int[][] table) {

        for (int i = 0; i < predicateList.size(); i++) {
            System.out.println((i + 1) + " " + predicateList.get(i));
        }
        System.out.println();

        int yDim = table[0].length;
        int xDim = table.length;
        for (int y = 0; y < yDim; y++) {
            String formatted = String.format("%03d", y + 1);
            System.out.print(formatted + " |");
            for (int x = 0; x < xDim - 1; x++) {
                System.out.print(" " + table[x][y] + " |");
            }
            String formattedValue = String.format("% 8d", table[xDim - 1][y]);
            System.out.println(formattedValue + " |");
        }
    }

    /**
     * Writes results to a csv file.
     *
     * @param file
     * @param predicateList
     * @param table
     * @throws FileNotFoundException
     */
    private static void writeToCSV(File file, ArrayList<String> predicateList, int[][] table) throws FileNotFoundException {
        PrintStream ps = new PrintStream(file);
        String divider = ";";

        for (int i = 0; i < predicateList.size(); i++) {
            ps.print(predicateList.get(i));
            if (i < predicateList.size() - 1) {
                ps.print(divider);
            }
        }
        ps.println();

        int yDim = table[0].length;
        int xDim = table.length;
        for (int y = 0; y < yDim; y++) {
            for (int x = 0; x < xDim; x++) {
                ps.print(table[x][y]);
                if (x < xDim - 1) {
                    ps.print(divider);
                }
            }
            ps.println();
        }
        ps.close();
    }

}
