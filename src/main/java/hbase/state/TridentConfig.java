package hbase.state;

import org.apache.storm.trident.state.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhulei on 2017/4/14.
 */
public class TridentConfig<T> extends TupleTableConfig {


    private int stateCacheSize = 1000;
    private Serializer stateSerializer;


    public  static final Map<StateType ,Serializer> DETAULT_SERIALIZER = new HashMap<StateType ,Serializer>(){{
        put(StateType.NON_TRANSACTIONAL,new JSONNonTransactionalSerializer());
        put(StateType.TRANSACTIONAL,new JSONTransactionalSerializer());
        put(StateType.OPAQUE,new JSONOpaqueSerializer());

    }};

    public TridentConfig(String tableName, String tupleRowKeyField) {
        super(tableName, tupleRowKeyField);
    }

    public TridentConfig(String tableName, String tupleRowKeyField, String tupleTimeStampField) {
        super(tableName, tupleRowKeyField, tupleTimeStampField);
    }

    public int getStateCacheSize() {
        return stateCacheSize;
    }

    public Serializer getStateSerializer() {
        return stateSerializer;
    }

    public void setStateCacheSize(int stateCacheSize) {
        this.stateCacheSize = stateCacheSize;
    }

    public void setStateSerializer(Serializer stateSerializer) {
        this.stateSerializer = stateSerializer;
    }

    public static Map<StateType, Serializer> getDetaultSerializer() {
        return DETAULT_SERIALIZER;
    }

}
