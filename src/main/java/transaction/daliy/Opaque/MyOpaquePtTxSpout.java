package transaction.daliy.Opaque;

import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.transactional.TransactionAttempt;
import org.apache.storm.transactional.partitioned.IOpaquePartitionedTransactionalSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import transcation.MyMeta;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created by zhulei on 2017/4/12.
 */
public class MyOpaquePtTxSpout implements IOpaquePartitionedTransactionalSpout<MyMeta> {
    private int BATCH_NUM = 10;
    public Map<Integer, Map<Long, String>> PT_DATA_MP = new HashMap<Integer, Map<Long, String>>();

    public MyOpaquePtTxSpout() {
        Random random = new Random();
        String[] hosts = {"www.taobao.com"};
        String[] session_id = {"1", "2", "3", "4", "5", "6"};
        String[] time = {"2014-01-07 08:40:50", "2014-01-07 08:40:51", "2014-01-07 08:40:52",
                "2014-01-07 09:40:49", "2014-01-07 10:40:49", "2014-01-07 11:40:49", "2014-01-07 12:40:49"};

        for (int j = 0; j < 5; j++) {

            Map<Long, String> dbMap = new HashMap<Long, String>();

            for (long i = 0; i < 100; i++) {
                dbMap.put(i, hosts[0] + "\t" + session_id[random.nextInt(5)] + "\t" + time[random.nextInt(7)]);
            }
            PT_DATA_MP.put(j, dbMap);
        }
    }

    @Override
    public Emitter<MyMeta> getEmitter(Map conf, TopologyContext context) {
        return new myEmitter();
    }

    @Override
    public Coordinator getCoordinator(Map conf, TopologyContext context) {
        return new MyCoordinator();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tx","log"));

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public class MyCoordinator implements Coordinator {
        @Override
        public boolean isReady() {
            Utils.sleep(1000);
            return true;
        }

        @Override
        public void close() {

        }
    }

    public class myEmitter implements Emitter<MyMeta> {
//      Return the metadata describing this batch that will be used as lastPartitionMeta
//      for defining the parameters of the next batch.
        @Override
        public MyMeta emitPartitionBatch(TransactionAttempt tx, BatchOutputCollector collector, int partition, MyMeta lastPartitionMeta) {

            long beginPoint = 0L;
            if (null == lastPartitionMeta) {
                beginPoint = 0;
            } else {
                beginPoint = lastPartitionMeta.getNum() + lastPartitionMeta.getBeginPoint();
            }
            MyMeta myMeta = new MyMeta();
            myMeta.setBeginPoint(beginPoint);
            myMeta.setNum(BATCH_NUM);
            System.err.println("启动一个事务   :" + myMeta);
            Map<Long, String> batchMap = PT_DATA_MP.get(partition);

            for (long i = myMeta.getBeginPoint(); i < myMeta.getBeginPoint() + myMeta.getNum(); i++) {
                if (batchMap.size() <= i) {
                    break;

                }
                collector.emit(new Values(tx,batchMap.get(i)));
            }
            //作为下一次循环的参数
            return myMeta;

        }

        @Override
        public int numPartitions() {
            return 5;
        }

        @Override
        public void close() {

        }
    }
}
