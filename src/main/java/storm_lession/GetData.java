package storm_lession;

import java.io.File;
import java.util.Random;

public class GetData {
	public static void main(String[] args) {
		Random random = new Random();
		String [] hosts = {"www.taobao.com"};
		String [] session_id  ={"1","2","3","4","5","6"};
		String [] time = {"2014-01-07 08:40:50","2014-01-07 08:40:51","2014-01-07 08:40:52",
				"2014-01-07 09:40:49","2014-01-07 10:40:49","2014-01-07 11:40:49","2014-01-07 12:40:49"};
		
		StringBuffer sb = new StringBuffer();
		
		for(int i=0;i<50;i++){
			sb.append(hosts[0]+"\t"+session_id[random.nextInt(5)]+"\t"+time[random.nextInt(8)]+"\n");
		}
	}
}
