package com.yinjz.flink.TableSQL_11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FakerSQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String createDDL = "CREATE TABLE doctor_sightings (\n" +
                "                doctor        STRING,\n" +
                "                sighting_time TIMESTAMP(3),\n" +
                "                WATERMARK FOR sighting_time AS sighting_time - INTERVAL '15' SECONDS\n" +
                ")\n" +
                "        WITH (\n" +
                "                'connector' = 'faker',\n" +
                "                'fields.doctor.expression' = '#{dr_who.the_doctors}',\n" +
                "                'fields.sighting_time.expression' = '#{date.past ''15'',''SECONDS''}'\n" +
                ");" ;
        String selectDML = "SELECT\n" +
                "                doctor,\n" +
                "        TUMBLE_ROWTIME(sighting_time, INTERVAL '1' MINUTE) AS sighting_time,\n" +
                "        COUNT(*) AS sightings\n" +
                "        FROM doctor_sightings\n" +
                "        GROUP BY\n" +
                "        TUMBLE(sighting_time, INTERVAL '1' MINUTE),\n" +
                "        doctor;" ;



        tableEnv.executeSql(createDDL);
        Table table = tableEnv.sqlQuery(selectDML);
        tableEnv.toChangelogStream(table).print(" ");


        env.execute();
    }
}
