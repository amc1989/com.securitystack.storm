package hbase.state;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;
import java.io.Serializable;


/**
 * Created by zhulei on 2017/4/14.
 */
public class HTableConnector implements Serializable {


    private Configuration configuration;
    private HTable table;
    private String tableName;

    public HTableConnector( TupleTableConfig tupleTableConfig) {
        this.tableName = tupleTableConfig.getTableName();
        this.configuration = HBaseConfiguration.create();

        String hbaseFilePath ="habse-site.xml";
        Path path = new Path(hbaseFilePath);
        this.configuration.addResource(path);
        try {
            this.table = new HTable(this.configuration,this.tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public HTable getTable() {
        return table;
    }

    public String getTableName() {
        return tableName;
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    public void setTable(HTable table) {
        this.table = table;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void close(){
        try {
            this.table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}