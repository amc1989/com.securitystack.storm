package transaction.daliy.Opaque;

import common.DateFmt;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.coordination.IBatchBolt;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.transactional.TransactionAttempt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhulei on 2017/4/11.
 */
public class MyDaliyBatchBolt implements IBatchBolt<TransactionAttempt> {

    private Integer count;
    private String today;
    private BatchOutputCollector collector;
    private Map<String, Integer> map = new HashMap<String, Integer>();
    private TransactionAttempt tx;

    @Override
    public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, TransactionAttempt id) {
        this.collector = collector;
    }
//处理每个tuple
    @Override
    public void execute(Tuple tuple) {
        String log = tuple.getString(1);
        tx = (TransactionAttempt) tuple.getValue(0);
        if (StringUtils.isNotBlank(log) && log.split("\t").length > 3) {
            today = DateFmt.getCountDate(log.split("\t")[2], DateFmt.date_short);
            count = map.get(today);
            if (null == count) {
                count = 0;
            }
            count++;
            map.put(today, count);
        }

    }
//当一个batch处理完的时候，一起commit
    @Override
    public void finishBatch() {

        collector.emit(new Values(tx, today, count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("tx", "today", "count"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
