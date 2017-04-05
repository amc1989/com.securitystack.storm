package visit;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Values;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Random;

public class SourceSpout implements IRichSpout {

    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private SpoutOutputCollector collector;
    private Queue<String> queue = new LinkedList<String>();

    @Override
    public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        Random random = new Random();
        String[] hosts = {"www.taobao.com"};
        String[] session_id = {"1", "2", "3", "4", "5", "6"};
        String[] time = {"2014-01-07 08:40:50", "2014-01-07 08:40:51", "2014-01-07 08:40:52",
                "2014-01-07 09:40:49", "2014-01-07 10:40:49", "2014-01-07 11:40:49", "2014-01-07 12:40:49"};


        for (int i = 0; i < 50; i++) {
            queue.add(hosts[0] + "\t" + session_id[random.nextInt(5)] + "\t" + time[random.nextInt(8)] + "\n");
        }

    }

    @Override
    public void close() {
        // TODO Auto-generated method stub

    }

    @Override
    public void activate() {
        // TODO Auto-generated method stub

    }

    @Override
    public void deactivate() {
        // TODO Auto-generated method stub

    }

    @Override
    public void nextTuple() {
        collector.emit(new Values(queue.poll()));

    }

    @Override
    public void ack(Object msgId) {
        // TODO Auto-generated method stub

    }

    @Override
    public void fail(Object msgId) {
        // TODO Auto-generated method stub

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // TODO Auto-generated method stub

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }

}
