package org.gesis.ntextract;

import java.io.BufferedWriter;
import java.io.IOException;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.rio.helpers.RDFHandlerBase;

/**
 * Implements a custom RDFHandler for RDFParser. Extracts the object URL from
 * every statement and writes it to System.out .
 *
 */
public class RDFWriterHandler extends RDFHandlerBase {

    private BufferedWriter bWriter = null;

    public RDFWriterHandler(BufferedWriter bWriter) {
        this.bWriter = bWriter;
    }

    @Override
    public void handleStatement(Statement st) {
        Value v = st.getObject();
        if (!(v instanceof org.openrdf.model.Literal)) {
            try {
                bWriter.write(v.stringValue());
                bWriter.newLine();
            } catch (IOException ex) {
                System.err.println(ex.getMessage());
                System.exit(-1);
            }
        }
    }

}
