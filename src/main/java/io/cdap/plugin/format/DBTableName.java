package io.cdap.plugin.format;

/**
 * Information about a DB table name.
 */
public class DBTableName {
  private String db;
  private String table;

  public DBTableName(String db, String table) {
    this.db = db;
    this.table = table;
  }

  public String getTable() {
    return table;
  }

  public String getDb() {
    return db;
  }

  public String fullTableName() {
    // this is required for oracle apparently? Don't know why
    return db == null ? table : db + "." + table;
  }

  public String fullTableNameQuoted() {
    return db == null ? "\"" + table + "\"" : "\"" + db + "\".\"" + table + "\"";
  }

}
