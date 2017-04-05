package visit;


import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.zookeeper.*;

import java.net.InetAddress;
import java.util.Map;

public class PVbolt implements IRichBolt {


    private OutputCollector collector;
    //在zookeeper主机上随便一个建/lock/storm目录
    private final String zk_path = "/lock/storm/pv";
    private ZooKeeper zooKeeper;
    private String lockData;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        try {
            zooKeeper = new ZooKeeper("192.168.159.10:2181,192.168.159.20:2181,192.168.159.30:2181", 5000, new Watcher() {
                @Override
                public void process(WatchedEvent watchedEvent) {
                    System.out.println(watchedEvent.getType());
                }
            });
            while (zooKeeper.getState() != ZooKeeper.States.CONNECTED) {
                Thread.sleep(1000);
            }
            InetAddress address = InetAddress.getLocalHost();
            lockData = address.getHostAddress() + ":" + context.getThisTaskId();

            if (zooKeeper.exists(zk_path, false) == null) {
                zooKeeper.create(zk_path, lockData.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            }


        } catch (Exception e) {
            try {
                zooKeeper.close();
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
        }
        this.collector = collector;
    }

    String logString = null;
    String session_id = null;

    //使用static关键词就能是数据对了
    //说明了storm的多线程是基于对象的，而不是基于方法的
//gmfkgjld
    //    static long pv = 0;
    long pv = 0;
    long startTime = System.currentTimeMillis();
    long endTime = 0l;

    @Override
    public void execute(Tuple input) {

        try {
            logString = input.getString(0);
            endTime = System.currentTimeMillis();

            if (!StringUtils.isEmpty(logString)) {
                session_id = logString.split("\t")[1];
            }

            if (session_id != null) {
                pv++;
            }

            if (endTime - startTime >= 5000) {

                if (lockData.equals(zooKeeper.getData(zk_path, false, null))) {

                    //pv*2 是基于 excutor的并发数
                    System.err.println("threadId   " + Thread.currentThread().getId() + "   pv   =" + pv * 2);
                }
                startTime = System.currentTimeMillis();
            }

            collector.emit(new Values(Thread.currentThread().getId(), pv));



        } catch (Exception e) {

        }

    }

    @Override
    public void cleanup() {
        // TODO Auto-generated method stub

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(""));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }

}
