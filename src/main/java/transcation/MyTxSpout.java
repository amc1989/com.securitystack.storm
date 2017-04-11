package transcation;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.transactional.ITransactionalSpout;
import org.apache.storm.tuple.Fields;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created by zhulei on 2017/4/10.
 *
 * 数据源
 */
public class MyTxSpout implements ITransactionalSpout<MyMeta> {

    private Map<Long, String> dbMap;

    public MyTxSpout() {
        dbMap = new HashMap<Long, String>();
        Random random = new Random();
        String[] hosts = {"www.taobao.com"};
        String[] session_id = {"1", "2", "3", "4", "5", "6"};
        String[] time = {"2014-01-07 08:40:50", "2014-01-07 08:40:51", "2014-01-07 08:40:52",
                "2014-01-07 09:40:49", "2014-01-07 10:40:49", "2014-01-07 11:40:49", "2014-01-07 12:40:49"};

        for (long i = 0; i < 100; i++) {
            dbMap.put(i, hosts[0] + "\t" + session_id[random.nextInt(5)] + "\t" + time[random.nextInt(7)]);
        }


    }

    @Override
    public Coordinator<MyMeta> getCoordinator(Map conf, TopologyContext context) {
        return new MyCoordinator();
    }

    @Override
    public Emitter<MyMeta> getEmitter(Map conf, TopologyContext context) {
        return   new MyEmitter(dbMap);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tx","log"));

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
