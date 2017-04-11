package transcation;


import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.transactional.ITransactionalSpout;
import org.apache.storm.transactional.TransactionAttempt;
import org.apache.storm.tuple.Values;

import java.math.BigInteger;
import java.util.Map;

/**
 * Created by zhulei on 2017/4/10.
 */
public class MyEmitter implements ITransactionalSpout.Emitter<MyMeta> {

    private Map<Long, String> dbMap;

    public MyEmitter(Map<Long, String> dbMap) {
        this.dbMap = dbMap;
    }


    @Override
    public void emitBatch(TransactionAttempt tx, MyMeta coordinatorMeta, BatchOutputCollector collector) {
        long beginPoint = coordinatorMeta.getBeginPoint();
        int num = coordinatorMeta.getNum();
        for (long i = beginPoint; i < num+beginPoint; i++) {
            if(null==dbMap.get(i)){
                continue;
            }
            collector.emit(new Values(tx, dbMap.get(i)));
            System.err.println("TransactionAttempt  MyEmitter: "+tx.getTransactionId()+"value :"+dbMap.get(i));
        }
    }

    @Override
    public void cleanupBefore(BigInteger txid) {

    }

    @Override
    public void close() {

    }
}
