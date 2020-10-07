/*
 * Copyright © 2017-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.format;


import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.DriverCleanup;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Record reader that reads the entire contents of a database table using JDBC.
 */
public class DBTableRecordReader extends RecordReader<NullWritable, StructuredRecord> {
  private final DBTableName tableName;
  private final String tableNameField;
  private final MultiTableConf dbConf;
  private final DriverCleanup driverCleanup;
  private DBTableSplit split;
  private int pos;
  private ResultSetMetaData resultMeta;
  private List<Schema.Field> tableFields;
  private Schema schema;
  private Connection connection;
  private Statement statement;
  private ResultSet results;

  DBTableRecordReader(MultiTableConf dbConf, DBTableName tableName, String tableNameField, DriverCleanup driverCleanup) {
    this.dbConf = dbConf;
    this.tableName = tableName;
    this.tableNameField = tableNameField;
    this.driverCleanup = driverCleanup;
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) {
    this.split = (DBTableSplit) split;
    this.pos = 0;
  }

  @Override
  public boolean nextKeyValue() throws IOException {
    try {
      if (results == null) {
        connection = dbConf.getConnection();
        statement = connection.createStatement();
        String query = getQuery();
        results = statement.executeQuery(query);
        resultMeta = results.getMetaData();
        tableFields = DBTypes.getSchemaFields(results);
        List<Schema.Field> schemaFields = new ArrayList<>(tableFields);
        schemaFields.add(Schema.Field.of(tableNameField, Schema.of(Schema.Type.STRING)));
        schema = Schema.recordOf(tableName.getTable(), schemaFields);
      }
      if (!results.next()) {
        return false;
      }

      pos++;
    } catch (SQLException e) {
      throw new IOException("SQLException in nextKeyValue", e);
    }
    return true;
  }

  @Override
  public NullWritable getCurrentKey() {
    return NullWritable.get();
  }

  @Override
  public StructuredRecord getCurrentValue() throws IOException {
    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(schema)
      .set(tableNameField, tableName.getTable());
    try {
      for (int i = 0; i < tableFields.size(); i++) {
        Schema.Field field = tableFields.get(i);
        int sqlColumnType = resultMeta.getColumnType(i + 1);
        DBTypes.setValue(recordBuilder, sqlColumnType, results, field.getName());
      }
    } catch (SQLException e) {
      throw new IOException("Error decoding row from table " + tableName, e);
    }
    return recordBuilder.build();
  }

  @Override
  public float getProgress() throws IOException {
    return pos / (float) split.getLength();
  }

  @Override
  public void close() throws IOException {
    SQLException exception = null;
    if (results != null) {
      try {
        results.close();
      } catch (SQLException e) {
        exception = e;
      }
    }

    if (statement != null) {
      try {
        statement.close();
      } catch (SQLException e) {
        if (exception != null) {
          exception.addSuppressed(e);
        } else {
          exception = e;
        }
      }
    }

    if (connection != null) {
      try {
        connection.commit();
        connection.close();
      } catch (SQLException e) {
        if (exception != null) {
          exception.addSuppressed(e);
        } else {
          exception = e;
        }
      }
    }

    driverCleanup.destroy();

    if (exception != null) {
      throw new IOException(exception);
    }
  }

  private String getQuery() {
    String query = "SELECT * FROM " + tableName.fullTableNameQuoted() + " ";
    String whereClause = dbConf.getWhereClause();

    if (whereClause != null && !whereClause.isEmpty()) {
      query += whereClause + " AND " + split.getWhereClause();
    } else {
      query += "WHERE " + split.getWhereClause();
    }

    return query;
  }
}
