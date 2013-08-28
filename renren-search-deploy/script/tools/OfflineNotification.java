import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.zookeeper.CreateMode;

public class OfflineNotification {
	public static void main(String[] args){
		String zkAddress="";
		String zkPath = "/search2/offline_attributes";
		if(args.length<1){
                        System.out.println("error, usage Java OfflineNotification zookeeper_address [path]");
                        System.exit(-1);
		}
		zkAddress = args[0];
		System.out.println("zkaddress : "+zkAddress);
		if(args.length==2){
			zkPath = args[1];
		}
		ZkConnection zkConnection= new ZkConnection(zkAddress);
		ZkClient zkclient=new ZkClient(zkConnection);
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		if(!zkclient.exists(zkPath)){
			System.out.println("path: "+zkPath+" doesn't exists, created");
			zkclient.create(zkPath, null, CreateMode.PERSISTENT);
		}
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("notification");
		zkclient.writeData(zkPath, "notification");
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.exit(0);
	}
}
