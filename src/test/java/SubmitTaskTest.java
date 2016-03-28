import java.io.InputStream;
import java.rmi.Naming;
import java.rmi.RemoteException;

import org.junit.Before;
import org.junit.Test;

import com.banana.common.master.ICrawlerMasterServer;

public class SubmitTaskTest {

	private ICrawlerMasterServer crawlerMasterServer;
	
	private String xmlConfig ;
	
	@Before
	public void init(){
		try {
			crawlerMasterServer = (ICrawlerMasterServer) Naming.lookup("rmi://localhost:1099/master");
			InputStream in = SubmitTaskTest.class.getClassLoader().getResourceAsStream("task_example.xml");
			byte[] data = new byte[in.available()];
			in.read(data, 0, data.length);
			xmlConfig = new String(data, "utf-8");
			in.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void submit(){
		System.out.println(xmlConfig);
		try {
			crawlerMasterServer.startTask(xmlConfig);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}
	
}
