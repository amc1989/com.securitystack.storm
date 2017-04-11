package transaction.daliy;

import org.apache.commons.lang3.StringUtils;
import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseTransactionalBolt;
import org.apache.storm.transactional.ICommitter;
import org.apache.storm.transactional.TransactionAttempt;
import org.apache.storm.tuple.Tuple;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhulei on 2017/4/10.
 */
public class MyDaliyCommiterBolt extends BaseTransactionalBolt implements ICommitter {

    //代表数据库
    private static Map<String, DBValue> dbMap = new HashMap<String, DBValue>();
    private static final String GLOBAL_KEY = "GLOBAL_KEY";
    private TransactionAttempt id;
    private BatchOutputCollector collector;
    private Map<String, Integer> countMap = new HashMap<String, Integer>();
    private String today ;

    @Override
    public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, TransactionAttempt id) {
        this.id = id;
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        today = tuple.getString(1);
        Integer count = tuple.getInteger(2);
        id = (TransactionAttempt) tuple.getValue(0);
        if (StringUtils.isNotBlank(today) && count != null) {
            Integer batchCount = countMap.get(today);
            if (null == batchCount) {
                batchCount = 0;
            }
            batchCount += count;
            countMap.put(today, batchCount);
        }
    }

    //事务的每个tuple完成了就通知comiitter去提交整个batch   finishBatch提交
    @Override
    public void finishBatch() {
        if(countMap.size()>0) {
            DBValue dbValue = dbMap.get(GLOBAL_KEY);
            DBValue newDBValue;
            if (null == dbValue || !id.getTransactionId().equals(dbValue.txid)) {
                //更新数据库
                newDBValue = new DBValue();
                newDBValue.txid = id.getTransactionId();
                newDBValue.dateStr = today;
                if (null == dbValue) {
                    newDBValue.count = countMap.get("2014-01-07");
                } else {
                    newDBValue.count = countMap.get(today) + dbValue.count;
                }
                dbMap.put(GLOBAL_KEY, newDBValue);
            } else {
                newDBValue = dbValue;
            }
        }
        System.err.println("total ===============:" + dbMap.get(GLOBAL_KEY).count);
        //        collector.emit(new Values(id.getTransactionId(), sum));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    public static class DBValue {
        BigInteger txid;
        int count = 0;
        String dateStr;
    }
}
