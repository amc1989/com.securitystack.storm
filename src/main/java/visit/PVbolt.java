package visit;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class PVbolt implements IRichBolt {

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        // TODO Auto-generated method stub

    }

    String logString =null;
    String session_id=null;
    //使用static关键词就能是数据对了
    //说明了storm的多线程是基于对象的，而不是基于方法的 
    static long pv=0;
    @Override
    public void execute(Tuple input) {
        logString = input.getString(0);
        session_id =  logString.split("\t")[1];
        if(session_id!=null){
            pv++;
        }
      System.err.println("pv   ="+pv);
    }

    @Override
    public void cleanup() {
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
