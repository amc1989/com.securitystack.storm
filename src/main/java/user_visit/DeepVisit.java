package user_visit;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhulei on 2017/4/6.
 */
public class DeepVisit implements IBasicBolt {


    @Override
    public void prepare(Map stormConf, TopologyContext context) {

    }

    Map<String,Integer> counts = new HashMap<String,Integer>();
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {

        String dateStr  = input.getString(0);
        String session_id = input.getString(1);
        Integer count = counts.get(dateStr+"_"+session_id);
         if(null==count){
             count = 0;
         }
         count++;
        System.err.println("DeepVisit       :"+dateStr+"_"+session_id +"   ,count  ："+count);
        counts.put(dateStr+"_"+session_id,count);

        collector.emit(new Values(dateStr+"_"+session_id,count));
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("date_session_id","count"));

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
