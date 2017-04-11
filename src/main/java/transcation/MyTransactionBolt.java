package transcation;

import org.apache.commons.lang3.StringUtils;
import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseTransactionalBolt;
import org.apache.storm.transactional.TransactionAttempt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by zhulei on 2017/4/10.
 */
public class MyTransactionBolt extends BaseTransactionalBolt {

    private Integer count=0;
    private BatchOutputCollector collector;
    private TransactionAttempt id;
    @Override
    public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, TransactionAttempt id) {

      this.collector = collector;
        this.id = id;
        System.err.println("MyTransactionBolt prepare" + id.getTransactionId() + " AttemptId " + id.getAttemptId());


    }

    //从myEmitter获取每个批次的每一个数据，处理完之后交由finishBatch处理
    @Override
    public void execute(Tuple tuple) {
        System.err.println("MyTransactionBolt    ::::"+tuple.getValue(0).getClass());
        TransactionAttempt tx = (TransactionAttempt) tuple.getValue(0);
        System.err.println("MyTransactionBolt Transactionid" + tx.getTransactionId() + " AttemptId " + tx.getAttemptId());
        String log = tuple.getString(1);
        if (StringUtils.isNotBlank(log))
             count++;


    }

    @Override
    public void finishBatch() {
        collector.emit(new Values(id,count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tx","counts"));
    }
}
