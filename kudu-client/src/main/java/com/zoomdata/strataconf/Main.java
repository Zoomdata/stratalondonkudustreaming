package com.zoomdata.strataconf;

import org.kududb.ColumnSchema;
import org.kududb.ColumnSchema.ColumnSchemaBuilder;
import org.kududb.Schema;
import org.kududb.Type;
import org.kududb.client.KuduClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.kududb.ColumnSchema.Encoding.DICT_ENCODING;

public class Main {

    private static final String KUDU_MASTER = System.getProperty("kuduMaster", "quickstart.cloudera");

    private static final String TABLE_NAME = System.getProperty("tableName", "strata_fruits_expanded");
    
    final static Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {

        log.info("-----------------------------------------------");
        log.info("Will try to connect to Kudu master at " + KUDU_MASTER);
        log.info("Run with -DkuduMaster=myHost:port to override.");
        log.info("-----------------------------------------------");

        final KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build();

        log.info(String.format("Will create table '%s'", TABLE_NAME));
        log.info("Run with -DtableName=TABLE_NAME to override");

        try {

            final boolean tableExists = client.tableExists(TABLE_NAME);
            if (tableExists) {
                log.info("Table already exists");
                System.exit(1);
            }

            final List<ColumnSchema> columns = new ArrayList<>();
            columns.add(new ColumnSchemaBuilder("_ts", Type.INT64).key(true).build());
            columns.add(new ColumnSchemaBuilder("_id", Type.STRING).key(true).build());
            columns.add(new ColumnSchemaBuilder("fruit", Type.STRING)
                    .encoding(DICT_ENCODING).build());
            columns.add(new ColumnSchemaBuilder("country_code", Type.STRING)
                    .encoding(DICT_ENCODING).build());
            columns.add(new ColumnSchemaBuilder("country_area_code", Type.STRING)
                    .encoding(DICT_ENCODING).build());
            columns.add(new ColumnSchemaBuilder("phone_num", Type.STRING).build());
            columns.add(new ColumnSchemaBuilder("message_date", Type.INT64).build());
            columns.add(new ColumnSchemaBuilder("price", Type.FLOAT).build());
            columns.add(new ColumnSchemaBuilder("keyword", Type.STRING).build());

            final Schema schema = new Schema(columns);
            client.createTable(TABLE_NAME, schema);

            log.info(String.format("Table '%s' successfully created", TABLE_NAME));

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                client.shutdown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}