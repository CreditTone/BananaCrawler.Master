package banana.master.serlvet;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.DecimalFormat;
import java.util.concurrent.TimeUnit;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.httpclient.HttpClient;
import org.openqa.selenium.remote.DesiredCapabilities;
import org.openqa.selenium.remote.RemoteWebDriver;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import banana.core.download.HttpDownloader;
import banana.core.download.impl.DefaultHttpDownloader;
import banana.core.request.HttpRequest;
import banana.core.request.PageRequest;
import banana.core.request.RequestBuilder;

public class TaobaoConvertLoanData2 extends HttpServlet {
	
	
	private static HttpDownloader httpDownloader = new DefaultHttpDownloader();
	
	private static String queryaccess_0_no ;
	private static String queryaccess_0_yes ;
	private static String queryaccess_1_no ;
	private static String queryaccess_1_yes ;
	private static String queryaccess_2_no ;
	private static String queryaccess_2_yes ;
	private static String queryaccess_3_no ;
	private static String queryaccess_3_yes ;
	
	static{
		//找一个全是没有额度的
		PageRequest request = (PageRequest) RequestBuilder.custom().setUrl("http://jie.file.site2/qr_taobao_shop/20170710/qr_taobao_shop_1499657312783614853/MY_CREDIT_queryaccess.ajax").build();
		String temp = httpDownloader.download(request).getContent();
		JSONObject queryaccess_json = JSON.parseObject(temp);
		JSONArray drawndnApplyEntryResult = queryaccess_json.getJSONArray("DrawndnApplyEntryResult");
		queryaccess_0_no = drawndnApplyEntryResult.getJSONObject(0).toJSONString();
		queryaccess_1_no = drawndnApplyEntryResult.getJSONObject(1).toJSONString();
		queryaccess_2_no = drawndnApplyEntryResult.getJSONObject(2).toJSONString();
		queryaccess_3_no = drawndnApplyEntryResult.getJSONObject(3).toJSONString();
		//找一个全有额度的
		request = (PageRequest) RequestBuilder.custom().setUrl("http://jie.file.site2/qr_taobao_shop/20170320/qr_taobao_shop_1489978869353491454/MY_CREDIT_queryaccess.ajax").build();
		temp = httpDownloader.download(request).getContent();
		queryaccess_json = JSON.parseObject(temp);
		drawndnApplyEntryResult = queryaccess_json.getJSONArray("DrawndnApplyEntryResult");
		queryaccess_0_yes = drawndnApplyEntryResult.getJSONObject(0).toJSONString();
		queryaccess_1_yes = drawndnApplyEntryResult.getJSONObject(1).toJSONString();
		queryaccess_2_yes = drawndnApplyEntryResult.getJSONObject(2).toJSONString();
		queryaccess_3_yes = drawndnApplyEntryResult.getJSONObject(3).toJSONString();
	}

	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
	}
	
	public static String replace(String input,String loanPrice){
		return replace("146200", loanPrice).replace("146,200", formatTosepara(loanPrice));
	}
	
	//把数字146200变成146,200
	 public static String formatTosepara(String data) {
	     DecimalFormat df = new DecimalFormat("#,###"); 
	     return df.format(Double.parseDouble(data));
	 }
	 
}
