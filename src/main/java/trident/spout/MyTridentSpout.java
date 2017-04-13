package trident.spout;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.ITridentSpout;
import org.apache.storm.trident.topology.TransactionAttempt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import transcation.MyMeta;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created by zhulei on 2017/4/13.
 */
public class MyTridentSpout implements ITridentSpout<MyMeta> {

    public MyTridentSpout() {

    }

    @Override
    public BatchCoordinator<MyMeta> getCoordinator(String txStateId, Map conf, TopologyContext context) {
        return null;
    }

    @Override
    public Emitter<MyMeta> getEmitter(String txStateId, Map conf, TopologyContext context) {
        return null;
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return null;
    }

    public  class myBatchCoordinateor implements  BatchCoordinator{
        @Override
        public Object initializeTransaction(long txid, Object prevMetadata, Object currMetadata) {
            return null;
        }

        @Override
        public void success(long txid) {

        }

        @Override
        public boolean isReady(long txid) {
            return true;
        }

        @Override
        public void close() {

        }
    }

    public class MyEmitter implements  Emitter{
        @Override
        public void emitBatch(TransactionAttempt tx, Object coordinatorMeta, TridentCollector collector) {

            Random random = new Random();
            String[] hosts = {"www.taobao.com"};
            String[] session_id = {"1", "2", "3", "4", "5", "6"};
            String[] time = {"2014-01-07 08:40:50", "2014-01-07 08:40:51", "2014-01-07 08:40:52",
                    "2014-01-07 09:40:49", "2014-01-07 10:40:49", "2014-01-07 11:40:49", "2014-01-07 12:40:49"};

            for (long i = 0; i < 100; i++) {
                collector.emit(new Values(hosts[0] + "\t" + session_id[random.nextInt(5)] + "\t" + time[random.nextInt(7)]) );
            }

        }

        @Override
        public void success(TransactionAttempt tx) {

        }

        @Override
        public void close() {

        }
    }
}
