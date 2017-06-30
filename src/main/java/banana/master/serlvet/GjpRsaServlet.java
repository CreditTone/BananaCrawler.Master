package banana.master.serlvet;

import java.io.IOException;
import java.math.BigInteger;
import java.net.URLEncoder;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.bouncycastle.util.encoders.Hex;

import com.alibaba.fastjson.JSONObject;

public class GjpRsaServlet extends HttpServlet {

	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		String modulusStr = "9A568982EE4BF010C38B5195A6F2DC7D66D5E6C02098CF25044CDD031AC08C6569D7063BB8959CB3FCB5AF572DE355AFA684AF7187948744E673275B494F394AF7F158841CA8B63BF65F185883F8D773A57ED731EDCD1AF2E0E57CD45F5F3CB4EBDD38F4A267E5ED02E7B44B93EDFFDADBDC8368019CD496BEC735BAF9E57125";
		String exponentStr = "010001";
		String pwd = req.getParameter("pwd");
		String encrypt = encryString(modulusStr, exponentStr, pwd);
		JSONObject json = new JSONObject();
		json.put("encrypt", encrypt);
		resp.getWriter().write(json.toJSONString());
	}
	
	
	 public static String encryString(String modulusStr,String exponentStr,String rawString) {
			if(rawString==null || rawString.length() ==0)
				return null;
			int digitSize = 128;
			int chunckSize = 128 - 11;
			BigInteger bigIntModulus0 = new BigInteger(modulusStr, 16);
			BigInteger bigIntPrivateExponent0 = new BigInteger(exponentStr, 16);
			StringBuffer newSB = new StringBuffer();
			String encodedString = URLEncoder.encode(rawString);
			
			//String testString = null;
			for(int i = 0; i < encodedString.length(); i+=chunckSize){
				int currentEndIndex = (i+chunckSize);
				if(currentEndIndex > encodedString.length() )
					currentEndIndex = encodedString.length();
				
				String currentString = encodedString.substring(0, currentEndIndex);
			
				byte[] sourceBytes = new byte[digitSize];
				int j=0;
				//CHECKPOINT  byte array length maynot be equal to chunckSize
				byte[] gottenBytes = currentString.getBytes();
				for(;j<gottenBytes.length;j++){
					sourceBytes[digitSize-j-1] = gottenBytes[gottenBytes.length-1-j];
				}
				//ENDCHECKPOINT
				sourceBytes[digitSize-j-1] = 0x00;
				j++;
				for(; j < digitSize -2;j++){
					sourceBytes[digitSize-j-1] = (byte)(Math.floor(Math.random() * 254) + 1);
				}
				sourceBytes[1] = 0x02;
				sourceBytes[0] = 0x00;
				
				String bigInt16String = new String(Hex.encode(sourceBytes));
				BigInteger bi01 = new BigInteger(bigInt16String,16);
				newSB.append(bi01.modPow(bigIntPrivateExponent0, bigIntModulus0).toString(16)).append(" ");
			}
			String rtnString = newSB.toString();
			if(rtnString.length() > 0){
				rtnString = rtnString.substring(0, rtnString.length()-1);
			}
			return rtnString;
		}
}
