package transcation;

import java.io.Serializable;

/**
 * Created by zhulei on 2017/4/10.
 */
public class MyMeta implements Serializable {


    private long beginPoint;//事务开始位置
    private int num;//batch的tuple个数

    public void setBeginPoint(long beginPoint) {
        this.beginPoint = beginPoint;
    }


    public void setNum(int num) {
        this.num = num;
    }

    public long getBeginPoint() {

        return beginPoint;
    }

    public int getNum() {
        return num;
    }

    @Override
    public String toString() {
        return "beginPoint    :" + beginPoint + "    ,num:" + num;
    }
}
