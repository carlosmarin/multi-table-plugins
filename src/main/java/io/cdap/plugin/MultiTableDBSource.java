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

package io.cdap.plugin;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.action.SettableArguments;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.common.SourceInputFormatProvider;
import io.cdap.plugin.format.DBTableInfo;
import io.cdap.plugin.format.MultiTableConf;
import io.cdap.plugin.format.MultiTableDBInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;

import java.sql.Driver;
import java.util.Collection;

/**
 * Batch source to read from multiple tables in a database using JDBC.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name("MultiTableDatabaseWithViews")
@Description("Reads from multiple tables in a relational database. " +
  "Outputs one record for each row in each table, with the table name as a record field. " +
  "Also sets a pipeline argument for each table read, which contains the table schema. ")
public class MultiTableDBSource extends BatchSource<NullWritable, StructuredRecord, StructuredRecord> {
  private static final String JDBC_PLUGIN_ID = "jdbc.driver";

  private final MultiTableConf conf;

  public MultiTableDBSource(MultiTableConf conf) {
    this.conf = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    Class<? extends Driver> jdbcDriverClass = pipelineConfigurer.usePluginClass("jdbc", conf.getJdbcPluginName(),
                                                                                JDBC_PLUGIN_ID,
                                                                                PluginProperties.builder().build());
    if (jdbcDriverClass == null) {
      throw new IllegalArgumentException(
        String.format("Unable to load JDBC Driver class for plugin name '%s'. " +
                        "Please make sure that the driver plugin has been installed correctly.",
                      conf.getJdbcPluginName()));
    }
    pipelineConfigurer.getStageConfigurer().setOutputSchema(null);
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    Configuration hConf = new Configuration();
    Class<? extends Driver> driverClass = context.loadPluginClass(JDBC_PLUGIN_ID);
    Collection<DBTableInfo> tables = MultiTableDBInputFormat.setInput(hConf, conf, driverClass);
    SettableArguments arguments = context.getArguments();
    for (DBTableInfo tableInfo : tables) {
      arguments.set(DynamicMultiFilesetSink.TABLE_PREFIX + tableInfo.getDbTableName().getTable(),
                    tableInfo.getSchema().toString());
    }

    context.setInput(Input.of(conf.getReferenceName(),
                              new SourceInputFormatProvider(MultiTableDBInputFormat.class, hConf)));
  }

  @Override
  public void transform(KeyValue<NullWritable, StructuredRecord> input, Emitter<StructuredRecord> emitter) {
    emitter.emit(input.getValue());
  }
}
