package trident.function;

import common.DateFmt;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

/**
 * Created by zhulei on 2017/4/13.
 */
public class MySplit extends BaseFunction {


    private String patton;

    public MySplit(String patton) {
        this.patton = patton;
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
          String sentence = tuple.getString(0);
        String[] split = sentence.split(patton);

        if(split.length ==3){
            collector.emit(new Values(DateFmt.getCountDate(split[2],DateFmt.date_short),"cf","pv_count",split[1]));
        }
    }
}
