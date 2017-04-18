package hbase.state;


import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.trident.state.OpaqueValue;
import org.apache.storm.trident.state.Serializer;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.state.StateType;
import org.apache.storm.trident.state.map.IBackingMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhulei on 2017/4/14.
 */
public class HbaseAggregateState<T> implements IBackingMap<T> {
    private HTableConnector connector;
    private Serializer serializer;


    public HbaseAggregateState(TridentConfig config) {
        this.serializer = config.getStateSerializer();
        this.connector = new HTableConnector(config);
    }


    public static HbaseAggregateFactory opaque(TridentConfig<OpaqueValue> config) {
        return new HbaseAggregateFactory(config, StateType.OPAQUE);
    }

    public static StateFactory transactional(TridentConfig config) {
        return new HbaseAggregateFactory(config, StateType.TRANSACTIONAL);
    }

    public static StateFactory nontransactional(TridentConfig config) {
        return new HbaseAggregateFactory(config, StateType.NON_TRANSACTIONAL);
    }


    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        List<Get> gets = new ArrayList<Get>(keys.size());
        byte[] rk;
        byte[] cf;
        byte[] cq;
        for (List<Object> k : keys) {

            rk = Bytes.toBytes((String) k.get(0));
            cf = Bytes.toBytes((String) k.get(1));
            cq = Bytes.toBytes((String) k.get(2));

            Get get = new Get(rk);
            gets.add(get.addColumn(cf, cq));
        }
        Result[] results = null;
        try {
            results = connector.getTable().get(gets);
        } catch (IOException e) {
            e.printStackTrace();
        }
        List<T> rtn = new ArrayList<T>(keys.size());
        for (int i = 0; i < keys.size(); i++) {
            cf = Bytes.toBytes((String) keys.get(i).get(1));
            cq = Bytes.toBytes((String) keys.get(i).get(2));
            Result result = results[i];
            if (result.isEmpty()) {
                rtn.add(null);
            } else {
                rtn.add((T) serializer.deserialize(result.getValue(cf, cq)));
            }
        }
        return rtn;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> vals) {
        List<Put> puts = new ArrayList<Put>();
        byte[] rk;
        byte[] cf;
        byte[] cq;
        for (int i = 0; i < keys.size(); i++) {
            rk = Bytes.toBytes((String) keys.get(i).get(0));
            cf = Bytes.toBytes((String) keys.get(i).get(1));
            cq = Bytes.toBytes((String) keys.get(i).get(2));
            byte[] cv = serializer.serialize(vals.get(i));
            Put put = new Put(rk);
            puts.add(put.add(cf, cq, cv));
        }
        try {
            connector.getTable().put(puts);
            connector.getTable().flushCommits();
        } catch (Exception e) {

        }
    }
}
