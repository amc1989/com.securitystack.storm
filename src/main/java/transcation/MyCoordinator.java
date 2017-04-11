package transcation;

import org.apache.storm.transactional.ITransactionalSpout;
import org.apache.storm.utils.Utils;

import java.math.BigInteger;

/**
 * Created by zhulei on 2017/4/10.
 */
public class MyCoordinator implements ITransactionalSpout.Coordinator<MyMeta> {


    private static int BATCH_NUM = 10;

    @Override
    public MyMeta initializeTransaction(BigInteger txid, MyMeta prevMetadata) {
        long beginPoint = 0L;
        if (null == prevMetadata) {
            beginPoint = 0;
        } else {
            beginPoint = prevMetadata.getNum() + prevMetadata.getBeginPoint();
        }
        MyMeta myMeta = new MyMeta();
        myMeta.setBeginPoint(beginPoint);
        myMeta.setNum(BATCH_NUM);

        System.err.println("启动一个事务   :" + myMeta);
        return myMeta;
    }

    @Override
    public boolean isReady() {
        Utils.sleep(2000);
        return true;
    }

    @Override
    public void close() {

    }
}
