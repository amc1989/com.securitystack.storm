package hbase.state;

import java.util.Map;
import java.util.Set;

/**
 * Created by zhulei on 2017/4/14.
 */
public class TupleTableConfig {


    private  String tableName;
    private  String tupleRowKeyField;
    private  String tupleTimeStampField;
    private Map<String,Set<String>> columFamilied;

    public TupleTableConfig() {
    }

    public TupleTableConfig(String tableName, String tupleRowKeyField, String tupleTimeStampField, Map<String, Set<String>> columFamilied) {
        this.tableName = tableName;
        this.tupleRowKeyField = tupleRowKeyField;
        this.tupleTimeStampField = tupleTimeStampField;
        this.columFamilied = columFamilied;
    }

    public TupleTableConfig(String tableName, String tupleRowKeyField) {
        this.tableName = tableName;
        this.tupleRowKeyField = tupleRowKeyField;
    }

    public TupleTableConfig(String tableName, String tupleRowKeyField, String tupleTimeStampField) {
        this.tableName = tableName;
        this.tupleRowKeyField = tupleRowKeyField;
        this.tupleTimeStampField = tupleTimeStampField;
    }

    public String getTableName() {
        return tableName;
    }

    public String getTupleRowKeyField() {
        return tupleRowKeyField;
    }

    public String getTupleTimeStampField() {
        return tupleTimeStampField;
    }

    public Map<String, Set<String>> getColumFamilied() {
        return columFamilied;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setTupleRowKeyField(String tupleRowKeyField) {
        this.tupleRowKeyField = tupleRowKeyField;
    }

    public void setTupleTimeStampField(String tupleTimeStampField) {
        this.tupleTimeStampField = tupleTimeStampField;
    }

    public void setColumFamilied(Map<String, Set<String>> columFamilied) {
        this.columFamilied = columFamilied;
    }

    @Override
    public String toString() {
        return "TupleTableConfig{" +
                "tableName='" + tableName + '\'' +
                ", tupleRowKeyField='" + tupleRowKeyField + '\'' +
                ", tupleTimeStampField='" + tupleTimeStampField + '\'' +
                ", columFamilied=" + columFamilied +
                '}';
    }
}
