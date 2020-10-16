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
import io.cdap.plugin.Drivers;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.db.BigDecimalSplitter;
import org.apache.hadoop.mapreduce.lib.db.BooleanSplitter;
import org.apache.hadoop.mapreduce.lib.db.DBSplitter;
import org.apache.hadoop.mapreduce.lib.db.DataDrivenDBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DateSplitter;
import org.apache.hadoop.mapreduce.lib.db.FloatSplitter;
import org.apache.hadoop.mapreduce.lib.db.IntegerSplitter;
import org.apache.hadoop.mapreduce.lib.db.TextSplitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Input format that reads from multiple tables in a database using JDBC. Similar to Hadoop's DBInputFormat.
 */
public class MultiTableDBInputFormat extends InputFormat<NullWritable, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(MultiTableDBInputFormat.class);
  private DriverCleanup driverCleanup;
  private Connection connection;

  /**
   * Configure the input format to read tables from a database. Should be called from the mapreduce client.
   *
   * @param hConf       the job configuration
   * @param dbConf      the database conf
   * @param driverClass the JDBC driver class used to communicate with the database
   * @return Collection of TableInfo containing DB, table and schema.
   */
  public static Collection<DBTableInfo> setInput(Configuration hConf, MultiTableConf dbConf,
                                                 Class<? extends Driver> driverClass) throws SQLException,
    InstantiationException, IllegalAccessException {

    MultiTableDBConfiguration multiTableDBConf = new MultiTableDBConfiguration(hConf);
    multiTableDBConf.setPluginConfiguration(dbConf);
    multiTableDBConf.setDriver(driverClass.getName());

    DriverCleanup cleanup = Drivers.ensureJDBCDriverIsAvailable(driverClass, dbConf.getConnectionString());

    try (Connection connection = dbConf.getConnection()) {
      DatabaseMetaData dbMeta = connection.getMetaData();
      ResultSet tables = dbMeta.getTables(null, dbConf.getSchemaNamePattern(), dbConf.getTableNamePattern(),
                                          new String[]{"TABLE", "TABLE_SCHEM", "VIEW"});
      List<DBTableInfo> tableInfos = new ArrayList<>();
      List<String> whiteList = dbConf.getWhiteList();
      List<String> blackList = dbConf.getBlackList();
      while (tables.next()) {
        String tableName = tables.getString("TABLE_NAME");
        // this is required for oracle apparently? Don't know why
        String db = tables.getString("TABLE_SCHEM");
        DBTableName dbTableName = new DBTableName(db, tableName);
        // If the table name exists in blacklist or when the whiteList is not empty and does not contain table name
        // the table should not be read
        if (!blackList.contains(tableName) && (whiteList.isEmpty() || whiteList.contains(tableName))) {
          List<String> primaryColumns = getPrimaryColumns(dbConf.getSchemaNamePattern(), tableName, dbMeta);
          Schema schema = getTableSchema(dbTableName.fullTableName(), connection);
          tableInfos.add(new DBTableInfo(dbTableName, schema, primaryColumns));
        }
      }
      multiTableDBConf.setTableInfos(tableInfos);
      return tableInfos;
    } finally {
      cleanup.destroy();
    }
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    MultiTableDBConfiguration conf = new MultiTableDBConfiguration(context.getConfiguration());
    MultiTableConf dbConf = conf.getPluginConf();

    int numSplit = 1;
    if (dbConf.getSplitsPerTable() != null) {
      numSplit = dbConf.getSplitsPerTable();
      conf.getConf().setInt(MRJobConfig.NUM_MAPS, numSplit);
    }

    List<DBTableInfo> tableInfos = conf.getTableInfos();
    List<InputSplit> resultSplits = new ArrayList<>();

    try (Connection connection = getConnection(conf)) {
      for (DBTableInfo info : tableInfos) {
        if (info.getPrimaryKey().size() != 1 || numSplit == 1) {
          resultSplits.add(new DBTableSplit(info.getDbTableName()));
        } else {
          resultSplits.addAll(getTableSplits(connection, conf, info));
        }
      }
    } catch (SQLException | IllegalAccessException | InstantiationException | ClassNotFoundException e) {
      throw new IOException(e);
    } finally {
      closeConnection();
    }

    return resultSplits;
  }

  @Override
  public RecordReader<NullWritable, StructuredRecord> createRecordReader(InputSplit split, TaskAttemptContext context)
    throws IOException {
    MultiTableDBConfiguration multiTableDBConf = new MultiTableDBConfiguration(context.getConfiguration());
    MultiTableConf dbConf = multiTableDBConf.getPluginConf();
    String driverClassname = multiTableDBConf.getDriverName();
    DBTableSplit dbTableSplit = (DBTableSplit) split;
    try {
      Class<? extends Driver> driverClass = (Class<? extends Driver>)
        multiTableDBConf.getConf().getClassLoader().loadClass(driverClassname);
      DriverCleanup driverCleanup = Drivers.ensureJDBCDriverIsAvailable(driverClass, dbConf.getConnectionString());
      return new DBTableRecordReader(dbConf, dbTableSplit.getTableName(), dbConf.getTableNameField(), driverCleanup);
    } catch (ClassNotFoundException e) {
      LOG.error("Could not load jdbc driver class {}", driverClassname);
      throw new IOException(e);
    } catch (IllegalAccessException | InstantiationException | SQLException e) {
      LOG.error("Could not register jdbc driver {}", driverClassname);
      throw new IOException(e);
    }
  }

  private Connection getConnection(MultiTableDBConfiguration multiTableDBConf) throws IllegalAccessException,
    SQLException, InstantiationException, ClassNotFoundException {
    if (connection == null) {
      MultiTableConf conf = multiTableDBConf.getPluginConf();
      Class<? extends Driver> driverClass = (Class<? extends Driver>) multiTableDBConf.getConf().getClassLoader()
        .loadClass(multiTableDBConf.getDriverName());
      driverCleanup = Drivers.ensureJDBCDriverIsAvailable(driverClass, conf.getConnectionString());
      connection = conf.getConnection();
    }

    return connection;
  }

  private void closeConnection() {
    try {
      if (null != this.connection) {
        if (!connection.isClosed()) {
          this.connection.close();
        }
        this.connection = null;
      }
    } catch (SQLException sqlE) {
      LOG.debug("Exception on close", sqlE);
    } finally {
      if (driverCleanup != null) {
        driverCleanup.destroy();
      }
    }
  }

  private List<InputSplit> getTableSplits(Connection connection, MultiTableDBConfiguration conf, DBTableInfo info)
    throws SQLException {
    String columnName = info.getPrimaryKey().get(0);
    try (Statement statement = connection.createStatement();
         ResultSet results = statement.executeQuery(getBoundingValsQuery(info.getDbTableName().fullTableName(),
                                                                         columnName,
                                                                         conf.getPluginConf().getWhereClause()))) {
      results.next();

      // Based on the type of the results, use a different mechanism
      // for interpolating split points (i.e., numeric splits, text splits,
      // dates, etc.)
      int sqlDataType = results.getMetaData().getColumnType(1);
      DBSplitter splitter = getSplitter(sqlDataType);
      if (null == splitter) {
        LOG.info("Failed to create internal splits for table " + info.getDbTableName().fullTableName() +
                   " only one split will be generated");
        return Collections.singletonList(new DBTableSplit(info.getDbTableName()));
      }
      return splitter.split(conf.getConf(), results, columnName)
        .stream()
        .map(split -> convertToDBTableSplit(info, split))
        .collect(Collectors.toList());
    }
  }

  private DBSplitter getSplitter(int sqlDataType) {
    switch (sqlDataType) {
      case Types.NUMERIC:
      case Types.DECIMAL:
        return new BigDecimalSplitter();

      case Types.BIT:
      case Types.BOOLEAN:
        return new BooleanSplitter();

      case Types.INTEGER:
      case Types.TINYINT:
      case Types.SMALLINT:
      case Types.BIGINT:
        return new IntegerSplitter();

      case Types.REAL:
      case Types.FLOAT:
      case Types.DOUBLE:
        return new FloatSplitter();

      case Types.CHAR:
      case Types.VARCHAR:
      case Types.LONGVARCHAR:
        return new TextSplitter();

      case Types.DATE:
      case Types.TIME:
      case Types.TIMESTAMP:
        return new DateSplitter();

      default:
        return null;
    }
  }

  private String getBoundingValsQuery(String tableName, String splitCol, String whereClause) {
    // Auto-generate one based on the table name we've been provided with.
    String query = "SELECT MIN(" + splitCol + "), MAX(" + splitCol + ") FROM ";
    return appendWhereClause(query, tableName, whereClause);
  }

  private static DBTableSplit convertToDBTableSplit(DBTableInfo info, InputSplit split) {
    DataDrivenDBInputFormat.DataDrivenDBInputSplit dataDrivenSplit =
      (DataDrivenDBInputFormat.DataDrivenDBInputSplit) split;
    return new DBTableSplit(info.getDbTableName(), dataDrivenSplit.getLowerClause(), dataDrivenSplit.getUpperClause());
  }

  private static String appendWhereClause(String selectClause, String table, String whereClause) {
    String query = selectClause + table;
    if (whereClause != null && !whereClause.isEmpty()) {
      query += " " + whereClause;
    }

    return query;
  }

  private static Schema getTableSchema(String table, Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      try (ResultSet results = statement.executeQuery("SELECT * FROM " + table + " WHERE 1 = 0")) {
        return Schema.recordOf(table, DBTypes.getSchemaFields(results));
      }
    }
  }

  private static List<String> getPrimaryColumns(String schema, String tableName, DatabaseMetaData metaData)
    throws SQLException {
    List<String> columnList = new ArrayList<>();
    ResultSet primaryColumns = metaData.getPrimaryKeys(null, schema, tableName);

    while (primaryColumns.next()) {
      columnList.add(primaryColumns.getString("COLUMN_NAME"));
    }

    return columnList;
  }
}
