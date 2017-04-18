package user_visit;

import common.DateFmt;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by zhulei on 2017/4/6.
 */
public class FmtLogBolt implements IBasicBolt{

    private String eachLog;


    @Override
    public void prepare(Map stormConf, TopologyContext context) {

    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        eachLog = input.getString(0);
        if(StringUtils.isNoneBlank(eachLog)){
            System.err.println("FmtLogBolt     :"+eachLog);
            collector.emit(new Values(DateFmt.getCountDate(eachLog.split("\t")[2],DateFmt.date_short),eachLog.split("\t")[1]));
        }


    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("date","session_id"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
