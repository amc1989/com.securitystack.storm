package user_visit;

import common.DateFmt;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.text.ParseException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by zhulei on 2017/4/6.
 */
public class UVSumBolt implements IBasicBolt {


    private Map<String,Integer> counts = new HashMap<String,Integer>();

    private  String cur_date ;
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
                cur_date = DateFmt.getCountDate(null,DateFmt.date_short);
    }
    private  long startTime = System.currentTimeMillis();
    private   long endTime = 0l;

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {

        try {
            endTime = System.currentTimeMillis();
            long pv = 0l;//总数
            long uv=0l;//去重后

            String  session_id = input.getString(0);
             Integer count = input.getInteger(1);


            if(session_id.startsWith(cur_date)&&DateFmt.parseDate(session_id.split(" ")[0]).after(DateFmt.parseDate(cur_date))){
                cur_date=session_id.split(" ")[0];
                counts.clear();
            }
            counts.put(session_id,count);

         /*   // 获得总数，遍历values，进行sum
            Iterator<Integer> values = counts.values().iterator();
            while(values.hasNext()){
                pv+=1;
            }*/

            //获取session_id去重个数，遍历map，
            if (endTime - startTime >= 2000) {
                Iterator<String> key = counts.keySet().iterator();
                while(key.hasNext()){

                    String session  = key.next();
                    if(StringUtils.isNotBlank(session)){
                        if(session.startsWith(cur_date)){
                            uv++;
                            pv+=counts.get(session);
                        }

                    }

                }
                System.err.println("uv   :"+uv+"   ;   pv     :"+pv);
            }



        } catch (ParseException e) {
            e.printStackTrace();
        }



    }

    @Override
    public void cleanup() {

                                                                        int i =0;
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
