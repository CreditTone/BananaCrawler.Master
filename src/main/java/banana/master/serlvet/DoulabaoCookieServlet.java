package banana.master.serlvet;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.TimeUnit;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.openqa.selenium.Cookie;
import org.openqa.selenium.remote.DesiredCapabilities;
import org.openqa.selenium.remote.RemoteWebDriver;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import banana.core.download.HttpDownloader;
import banana.core.download.impl.DefaultHttpDownloader;
import banana.core.request.PageRequest;
import banana.core.request.RequestBuilder;

public class DoulabaoCookieServlet extends HttpServlet {
	
	
	private static RemoteWebDriver webDriver = null;
	private static HttpDownloader httpDownloader = new DefaultHttpDownloader();
	
	private static void initWebDriver(){
		String sessionId = webDriver!=null?webDriver.getSessionId().toString():"null";
		PageRequest request = (PageRequest)  RequestBuilder.custom().setUrl("http://127.0.0.1:8888/wd/hub/authcrawler/sessioncheck?sessionId="+sessionId).build();
		boolean exist = JSON.parseObject(httpDownloader.download(request).getContent()).getBoolean("exist");
		if (!exist){
			try {
				//The browser version, or the empty string if unknown.
				DesiredCapabilities desiredCapabilities = DesiredCapabilities.phantomjs();
				//desiredCapabilities.setCapability(InternetExplorerDriver.INTRODUCE_FLAKINESS_BY_IGNORING_SECURITY_DOMAINS, true);
				webDriver = new RemoteWebDriver(new URL("http://127.0.0.1:8888/wd/hub"),desiredCapabilities);
//				webDriver.manage().timeouts().implicitlyWait(30, TimeUnit.SECONDS);//组件查找超时
//				webDriver.manage().timeouts().pageLoadTimeout(30, TimeUnit.SECONDS);//页面加载超时
				webDriver.manage().timeouts().setScriptTimeout(10, TimeUnit.SECONDS);//脚步执行超时
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
	}
	
	public static void main(String[] args) {
		initWebDriver();
		webDriver.get("http://www.duolabao.cn/");
		for (Cookie cookie:webDriver.manage().getCookies()) {
			System.out.println(cookie.getName());
		}
		webDriver.quit();
	}

	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		synchronized (this) {
			initWebDriver();
			webDriver.get("http://www.duolabao.cn/");
			JSONObject ret = new JSONObject();
			for (Cookie cookie:webDriver.manage().getCookies()) {
				ret.put(cookie.getName(), cookie.getValue());
			}
			resp.getWriter().write(ret.toJSONString());
		}
	}
	
	@Override
	public void destroy() {
		super.destroy();
		if (webDriver != null){
			webDriver.quit();
		}
	}
	
}
