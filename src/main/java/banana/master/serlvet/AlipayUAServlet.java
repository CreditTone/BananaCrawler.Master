package banana.master.serlvet;

import java.io.IOException;
import java.math.BigInteger;
import java.net.URL;
import java.net.URLEncoder;
import java.util.concurrent.TimeUnit;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.bouncycastle.util.encoders.Hex;
import org.openqa.selenium.remote.DesiredCapabilities;
import org.openqa.selenium.remote.RemoteWebDriver;

import com.alibaba.fastjson.JSONObject;

public class AlipayUAServlet extends HttpServlet {
	
	
	private static RemoteWebDriver webDriver = null;
	
	private static void initWebDriver(){
		try {
			//The browser version, or the empty string if unknown.
			DesiredCapabilities desiredCapabilities = DesiredCapabilities.firefox();
			//desiredCapabilities.setCapability(InternetExplorerDriver.INTRODUCE_FLAKINESS_BY_IGNORING_SECURITY_DOMAINS, true);
			webDriver = new RemoteWebDriver(new URL("http://127.0.0.1:4444/wd/hub"),desiredCapabilities);
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
		webDriver.get("https://consumeprod.alipay.com/record/advanced.htm");
	}

	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		String form_tk = req.getParameter("form_tk");
		webDriver.executeScript("UA_Opt.Token='"+form_tk+"'");
		webDriver.executeScript("UA_Opt.reload()");
		System.out.println(webDriver.executeScript("return UA_Opt"));
		Object json_ua = webDriver.executeScript("return json_ua");
		Object token = webDriver.executeScript("return UA_Opt.Token");
		JSONObject ret = new JSONObject();
		ret.put("json_ua", json_ua);
		ret.put("token", token);
		resp.getWriter().write(ret.toJSONString());
	}

	
	
	@Override
	public void destroy() {
		super.destroy();
		webDriver.quit();
	}
	
}
