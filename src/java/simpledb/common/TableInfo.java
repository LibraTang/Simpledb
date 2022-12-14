package simpledb.common;

import simpledb.storage.DbFile;

public class TableInfo {
    private int tableId;
    private String tableName;
    private DbFile dbFile;
    private String pkeyField;

    public TableInfo(int tableId, String tableName, DbFile dbFile, String pkeyField) {
        this.tableId = tableId;
        this.tableName = tableName;
        this.dbFile = dbFile;
        this.pkeyField = pkeyField;
    }

    public int getTableId() {
        return tableId;
    }

    public void setTableId(int tableId) {
        this.tableId = tableId;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public DbFile getDbFile() {
        return dbFile;
    }

    public void setDbFile(DbFile dbFile) {
        this.dbFile = dbFile;
    }

    public String getPkeyField() {
        return pkeyField;
    }

    public void setPkeyField(String pkeyField) {
        this.pkeyField = pkeyField;
    }
}
