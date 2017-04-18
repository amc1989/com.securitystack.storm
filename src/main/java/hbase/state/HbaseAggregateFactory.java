package hbase.state;

import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.OpaqueValue;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.state.StateType;
import org.apache.storm.trident.state.map.*;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by zhulei on 2017/4/14.
 */
public class HbaseAggregateFactory implements StateFactory {

    private StateType type;
    private TridentConfig config;

    public HbaseAggregateFactory(TridentConfig<OpaqueValue> config, StateType opaque) {
        this.type = type;
        this.config = config;
        if (config.getStateSerializer() == null) {
            config.setStateSerializer(TridentConfig.DETAULT_SERIALIZER.get(type));
        }
    }


    @Override
    public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {

        HbaseAggregateState state = new HbaseAggregateState(config);
        CachedMap c = new CachedMap(state,config.getStateCacheSize());
        MapState ms = null;
        if(type==StateType.NON_TRANSACTIONAL){
            ms = NonTransactionalMap.build(c);
        }else if(type==StateType.OPAQUE){
            ms = OpaqueMap.build(c);
        }else if(type==StateType.TRANSACTIONAL){
            ms = TransactionalMap.build(c);
        }

        return new SnapshottableMap(ms,new Values("$GLOBAL$"));
    }


}
