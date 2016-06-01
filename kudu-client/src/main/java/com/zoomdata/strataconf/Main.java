package com.zoomdata.strataconf;

import org.kududb.ColumnSchema;
import org.kududb.ColumnSchema.ColumnSchemaBuilder;
import org.kududb.Schema;
import org.kududb.client.CreateTableOptions;
import org.kududb.client.KuduClient;
import org.kududb.client.KuduTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.kududb.ColumnSchema.Encoding.DICT_ENCODING;
import static org.kududb.Type.FLOAT;
import static org.kududb.Type.INT64;
import static org.kududb.Type.STRING;

public class Main {

    private static final String KUDU_MASTER = System.getProperty("kuduMaster", "quickstart.cloudera");

    private static final String TABLE_NAME = System.getProperty("tableName", "strata_fruits_expanded");

    private static final Integer BUCKETS_NUM = Integer.valueOf(System.getProperty("bucketsNum", "60"));

    private static final Integer REPLICAS_NUM = Integer.valueOf(System.getProperty("replicaNum", "3"));

    final static Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {

        log.info("-----------------------------------------------");
        log.info("Will try to connect to Kudu master at " + KUDU_MASTER);
        log.info("Run with -DkuduMaster=myHost:port to override.");
        log.info("-----------------------------------------------");

        final KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build();
        createTable(client, TABLE_NAME, BUCKETS_NUM, REPLICAS_NUM);
        log.info(String.format("Table '%s' successfully created", TABLE_NAME));
    }

    private static KuduTable createTable(
            final KuduClient client, final String tableName, final int bucketsNum, final int replicas) throws Exception {

        log.info(String.format("Will create table '%s'", tableName));
        log.info("Run with -DtableName=TABLE_NAME to override");

        try {

            final boolean tableExists = client.tableExists(tableName);
            if (tableExists) {
                log.info("Table already exists");
                System.exit(1);
            }

            final List<ColumnSchema> columns = new ArrayList<>();
            columns.add(new ColumnSchemaBuilder("_ts", INT64).key(true).build());
            columns.add(new ColumnSchemaBuilder("_id", STRING).key(true).build());
            columns.add(new ColumnSchemaBuilder("fruit", STRING)
                    .encoding(DICT_ENCODING).build());
            columns.add(new ColumnSchemaBuilder("country_code", STRING)
                    .encoding(DICT_ENCODING).build());
            columns.add(new ColumnSchemaBuilder("country_area_code", STRING)
                    .encoding(DICT_ENCODING).build());
            columns.add(new ColumnSchemaBuilder("phone_num", STRING).build());
            columns.add(new ColumnSchemaBuilder("message_date", INT64).build());
            columns.add(new ColumnSchemaBuilder("price", FLOAT).build());
            columns.add(new ColumnSchemaBuilder("keyword", STRING).build());

            final Schema schema = new Schema(columns);
            final CreateTableOptions tableOptions = new CreateTableOptions();

            String[] partitionColumns = {"_ts"};
            log.info("Using hash partitions for {} columns, {} replicas and {} buckets",
                    partitionColumns, replicas, bucketsNum);
            tableOptions.addHashPartitions(Arrays.asList(partitionColumns), bucketsNum);
            tableOptions.setNumReplicas(replicas);

            return client.createTable(tableName, schema, tableOptions);

        } finally {
            client.shutdown();
        }
    }
}