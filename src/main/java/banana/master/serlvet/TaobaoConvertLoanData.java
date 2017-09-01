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
import com.alibaba.fastjson.JSONObject;

import banana.core.download.HttpDownloader;
import banana.core.download.impl.DefaultHttpDownloader;
import banana.core.request.HttpRequest;
import banana.core.request.PageRequest;
import banana.core.request.RequestBuilder;

public class TaobaoConvertLoanData extends HttpServlet {
	
	
	private static HttpDownloader httpDownloader = new DefaultHttpDownloader();
	
	private static String queryorder_no ;
	private static String queryorder_yes ;
	
	static{
		PageRequest request = (PageRequest) RequestBuilder.custom().setUrl("http://jie.file.site2/qr_tmall_shop/20170313/qr_tmall_shop_1489378002633102477/MY_CREDIT_queryorder.ajax").build();
		queryorder_no = httpDownloader.download(request).getContent();
		request = (PageRequest) RequestBuilder.custom().setUrl("http://jie.file.site2/qr_taobao_shop/20170320/qr_taobao_shop_1489978869353491454/MY_CREDIT_queryorder.ajax").build();
		queryorder_yes = httpDownloader.download(request).getContent();
	}

	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		String loanPrice = req.getParameter("loanPrice");
		double intLoanPrice = Double.parseDouble(loanPrice);
		String ret = null;
		if (loanPrice.length() > 2){
			 ret = queryorder_yes.replace("146200", loanPrice).replace("146,200", formatTosepara(intLoanPrice));
		}else{
			 ret = queryorder_no;
		}
		resp.getWriter().write(ret);
	}
	
	 public static String formatTosepara(double data) {
	     DecimalFormat df = new DecimalFormat("#,###"); 
	     return df.format(data);
	 }
	 
}
