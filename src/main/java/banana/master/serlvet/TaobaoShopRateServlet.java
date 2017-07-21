package banana.master.serlvet;

import java.io.IOException;
import java.net.URL;
import java.util.concurrent.TimeUnit;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.openqa.selenium.remote.DesiredCapabilities;
import org.openqa.selenium.remote.RemoteWebDriver;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class TaobaoShopRateServlet extends HttpServlet {
	
	
	private static RemoteWebDriver webDriver = null;
	
	private static void initWebDriver(){
		try {
			//The browser version, or the empty string if unknown.
			DesiredCapabilities desiredCapabilities = DesiredCapabilities.phantomjs();
			//desiredCapabilities.setCapability(InternetExplorerDriver.INTRODUCE_FLAKINESS_BY_IGNORING_SECURITY_DOMAINS, true);
			webDriver = new RemoteWebDriver(new URL("http://127.0.0.1:8888/wd/hub"),desiredCapabilities);
//			webDriver.manage().timeouts().implicitlyWait(30, TimeUnit.SECONDS);//组件查找超时
//			webDriver.manage().timeouts().pageLoadTimeout(30, TimeUnit.SECONDS);//页面加载超时
			webDriver.manage().timeouts().setScriptTimeout(10, TimeUnit.SECONDS);//脚步执行超时
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void init() throws ServletException {
		super.init();
		initWebDriver();
		webDriver.get("https://rate.taobao.com/user-rate-UvFNLvmcuOFvuvWTT.htm?spm=a1z10.1-c-s.0.0.41fe2e6aGCRrHp");
	}

	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		String userNumId = req.getParameter("userNumId");
		String shopID = req.getParameter("shopID");
		webDriver.get("https://rate.taobao.com/ShopService4C.htm?userNumId="+userNumId+"&shopID="+shopID+"&isB2C=true");
		String content = webDriver.getPageSource();
		JSONObject ret = new JSONObject();
		if (content.contains("avgRefund")){
			int startIndex = content.indexOf("{");
			int endIndex = content.lastIndexOf("}")+1;
			content = content.substring(startIndex, endIndex);
			ret = JSON.parseObject(content);
		}
		resp.getWriter().write(ret.toJSONString());
	}

	
	
	@Override
	public void destroy() {
		super.destroy();
		webDriver.quit();
	}
	
}
