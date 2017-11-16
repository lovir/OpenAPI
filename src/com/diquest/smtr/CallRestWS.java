package com.diquest.smtr;

import java.io.InputStream;
import java.net.URL;
import java.net.URLEncoder;
import org.apache.cxf.helpers.IOUtils;
import org.apache.cxf.io.CachedOutputStream;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;


@SuppressWarnings("unused")
public class CallRestWS {
    String driver        = "org.mariadb.jdbc.Driver";
    String url           = "jdbc:mariadb://172.22.11.16:3307/metro_bigdb";
    String uId           = "diquest";
    String uPwd          = "ek2znptm2!@";
    //어제 날짜 08시
    String lastDay 		= "";
    String obsrDe		= ""; 
    
    Connection               con;
    PreparedStatement        pstmt;
    ResultSet                rs;
    
    public CallRestWS() {
    	Calendar cal = new GregorianCalendar();
    	cal.add(Calendar.DATE, -1);
    	SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
    	
    	lastDay = sdf.format(cal.getTime());
    	obsrDe = lastDay + "08";
    	
    	System.out.println("lastDay ::: " + lastDay);
    			
        try {
           Class.forName(driver);
           con = DriverManager.getConnection(url, uId, uPwd);
           
           if( con != null ){ System.out.println("데이터 베이스 접속 성공"); }
           
       } catch (ClassNotFoundException e) { System.out.println("드라이버 로드 실패");    } 
         catch (SQLException e) { System.out.println("데이터 베이스 접속 실패"); }
   }



	// 기상 관측 데이터 OpenAPI
	private String restClientDailyWeather() throws Exception{
		String addr = "http://openapi.seoul.go.kr:8088/";
		String serviceKey = "4173464d746c6f7634384a576b7269";
		String parameter = "/json/DailyWeatherStation/1/50/" + lastDay;
		
		//인증키(서비스키) url인코딩
		serviceKey = URLEncoder.encode(serviceKey, "UTF-8");
		
		/* parameter setting
		 * parameter = parameter + "&" + "PARAM1=AAA";
		 * parameter = parameter + "&" + "PARAM2=BBB";
		 * parameter = parameter + "&" + "PARAM3=CCC";
		 * */
		
		addr = addr + serviceKey + parameter;
		
		System.out.println(addr);
		
		URL url = new URL(addr);
		InputStream in = url.openStream(); 
		CachedOutputStream bos = new CachedOutputStream();
		IOUtils.copy(in, bos);
		in.close();
		bos.close();
		return bos.getOut().toString();
	}
	
		
	public String preCheck(){
        String sql    = "select count(*) as co from ST_VOC013_L where OBSR_DE='" + lastDay + "'";
        try {
            pstmt                = con.prepareStatement(sql);
            rs                   = pstmt.executeQuery();
            while(rs.next()){
                System.out.println("Count       : " + rs.getInt("co"));
                
                if (rs.getInt("co") > 20)
                	return "exist";
                else
                	return "noExist";
            }
            
            pstmt.close();
    		
        } catch (SQLException e) { System.out.println("쿼리 수행 실패"); }  
        
        
        return "noExist";
    }
	
	public JSONArray apiCheck(String apiData){
		
		 try {
	            JSONParser jsonParser = new JSONParser();
	            JSONObject jsonObj = (JSONObject) jsonParser.parse(apiData);
	            String resultCode = "";
	            
	            JSONObject ds = (JSONObject) jsonObj.get("DailyWeatherStation");
	            
	            JSONObject result = (JSONObject) ds.get("RESULT");
	            
	            System.out.println("CODE : "+ result.get("CODE"));
	            
	            resultCode = result.get("CODE").toString();
	            
	            if (resultCode.equals("INFO-000"))
	            {
	            	JSONArray wheatherArray = (JSONArray) ds.get("row");
	            
		            //System.out.println("=====row=====");
		            /*for(int i=0 ; i<memberArray.size() ; i++){
		                JSONObject tempObj = (JSONObject) memberArray.get(i);
		                System.out.println(""+(i+1)+"번째 지역의 평균기온 : "+tempObj.get("SAWS_TA_AVG"));
		                System.out.println(""+(i+1)+"번째 지역의 최저기온 : "+tempObj.get("SAWS_TA_MIN"));
		                System.out.println(""+(i+1)+"번째 지역의 최고기온 : "+tempObj.get("SAWS_TA_MAX"));
		                System.out.println("----------------------------");
		            }*/
	            
	            	return wheatherArray;
	            }

	        } catch (ParseException e) {
	            // TODO Auto-generated catch block
	            e.printStackTrace();
	        }
		 
		return null;		
		
	}
	
	public String dbInsert(JSONArray weatherArray){
		
		String sql    = "insert into ST_VOC013_L (OBSR_DE, SPOT_NM, AVRG_TMPRT, LWET_TMPRT, TOP_TMPRT, AVRG_HD, LWET_HD, TOP_HD, AVRG_WS, MXMM_WS, PRCPTQY, REGIST_DT, RESULT_CODE) values (?,?,?,?,?,?,?,?,?,?,?, now(),'INFO-000')";     
        
        try {
        	for(int i=0 ; i < weatherArray.size() ; i++) {
        		JSONObject tempObj = (JSONObject) weatherArray.get(i);
        		
        		pstmt = con.prepareStatement(sql);            
        		pstmt.setString(1, tempObj.get("SAWS_OBS_TM").toString());
        		pstmt.setString(2, tempObj.get("STN_NM").toString());
        		pstmt.setDouble(3, (double) tempObj.get("SAWS_TA_AVG"));
        		pstmt.setDouble(4, (double)tempObj.get("SAWS_TA_MIN"));
        		pstmt.setDouble(5, (double)tempObj.get("SAWS_TA_MAX"));
        		pstmt.setDouble(6, (double)tempObj.get("SAWS_HD_AVG"));
        		pstmt.setDouble(7, (double)tempObj.get("SAWS_HD_MIN"));
        		pstmt.setDouble(8, (double)tempObj.get("SAWS_HD_MAX"));
        		pstmt.setDouble(9, (double)tempObj.get("SAWS_WS_AVG"));
        		pstmt.setDouble(10, (double)tempObj.get("SAWS_WS_MAX"));
        		pstmt.setDouble(11, (double)tempObj.get("SAWS_RN_SUM"));
        		
        		pstmt.executeUpdate();
        	}            
        	
            pstmt.close();

        } catch (SQLException e) { System.out.println("쿼리 수행 실패"); }
        
        return "";
    }



	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		CallRestWS crw = new CallRestWS();
		String ret = "";
		JSONArray weatherArray = null;
		
		String preCheckResult = "";
		
		// 01. 기존 수집 여부 체크
		preCheckResult = crw.preCheck();				
		
		// 02. OpenAPI 데이터 수신
		
		if (preCheckResult.equals("exist"))	// 수집이 완료되었다면 프로그램 종료
		{
			System.exit(0);
		} else {
			
			ret = crw.restClientDailyWeather();
			weatherArray = crw.apiCheck(ret);			
		}	
				
		// 03. DB Insert 및 체크
		
		if (weatherArray == null){			// 수신된 데이터에 이상 있음
			System.exit(1);
		}else{								// DB Insert
			
			crw.dbInsert(weatherArray);
			
		}
		
		// 04. 종료
		crw.con.close();			
	}

}
