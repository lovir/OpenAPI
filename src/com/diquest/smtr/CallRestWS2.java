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

import org.json.JSONException;
import org.json.XML;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;


@SuppressWarnings("unused")
public class CallRestWS2 {
    String driver        = "org.mariadb.jdbc.Driver";
    String url           = "jdbc:mariadb://172.22.11.16:3307/metro_bigdb";
    String uId           = "diquest";
    String uPwd          = "ek2znptm2!@";
    //어제 날짜 08시
    String toDay 		= "";
    String obsrDe		= "";
    
    ArrayList<String> frcstDayList = null;
    
    String [] day = new String[8];
    
    Connection               con;
    PreparedStatement        pstmt, pstmt2;
    ResultSet                rs, rs2;
    
    public CallRestWS2() {
    	Calendar cal = new GregorianCalendar();
    	//cal.add(Calendar.DATE, -1);
    	SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
    	
    	toDay = sdf.format(cal.getTime());
    	obsrDe = toDay + "0600";
    	
    	cal.add(Calendar.DATE, 3);
    	day[0] = sdf.format(cal.getTime());
    	cal.add(Calendar.DATE, 1);
    	day[1] = sdf.format(cal.getTime());
    	cal.add(Calendar.DATE, 1);
    	day[2] = sdf.format(cal.getTime());
    	cal.add(Calendar.DATE, 1);
    	day[3] = sdf.format(cal.getTime());
    	cal.add(Calendar.DATE, 1);
    	day[4] = sdf.format(cal.getTime());
    	cal.add(Calendar.DATE, 1);
    	day[5] = sdf.format(cal.getTime());
    	cal.add(Calendar.DATE, 1);
    	day[6] = sdf.format(cal.getTime());
    	cal.add(Calendar.DATE, 1);
    	day[7] = sdf.format(cal.getTime());
    	
    	System.out.println("toDay ::: " + toDay);
    			
        try {
           Class.forName(driver);
           con = DriverManager.getConnection(url, uId, uPwd);
           
           if( con != null ){ System.out.println("데이터 베이스 접속 성공"); }
           
       } catch (ClassNotFoundException e) { System.out.println("드라이버 로드 실패");    } 
         catch (SQLException e) { System.out.println("데이터 베이스 접속 실패"); }
   }
	
	
	// 중기 육상 예보 OpenAPI
	private String restClientMiddleLandWeather() throws Exception{
			String addr = "http://newsky2.kma.go.kr/service/MiddleFrcstInfoService/getMiddleLandWeather?serviceKey=";
			String serviceKey = "3T2UHYxd%2FQ5xBwjoaQe24orCYW6Jgeq5MtNEe6FBZuZFJrG7WOF2H2PIeCNvb28opxJjYDyOK2i3oMkvYxvv0A%3D%3D";
			String parameter = "&regId=11B00000&tmFc=" + obsrDe + "&numOfRows=10&pageSize=10&pageNo=1&startPage=1";
		
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
	
	// 중기 기온 예보  OpenAPI
	private String restClientMiddleTemperature() throws Exception{
				String addr = "http://newsky2.kma.go.kr/service/MiddleFrcstInfoService/getMiddleTemperature?serviceKey=";
				String serviceKey = "3T2UHYxd%2FQ5xBwjoaQe24orCYW6Jgeq5MtNEe6FBZuZFJrG7WOF2H2PIeCNvb28opxJjYDyOK2i3oMkvYxvv0A%3D%3D";
				String parameter = "&regId=11B10101&tmFc=" + obsrDe + "&numOfRows=10&pageSize=10&pageNo=1&startPage=1";
								
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
        String sql    = "select count(*) co from ST_VOC014_L where DATE_FORMAT(REGIST_DT,'%Y%m%d') = DATE_FORMAT(NOW(),'%Y%m%d') and RESULT_CODE='0000'";
		String checkSql = "select FRCST_DE from ST_VOC014_L order by SEQ_NO desc limit 10";
		frcstDayList = new ArrayList<String>();
		String returnValue = "noExist";
		
        try {
            pstmt                = con.prepareStatement(sql);
            rs                   = pstmt.executeQuery();
            while(rs.next()){
                System.out.println("Count       : " + rs.getInt("co"));
                
                if (rs.getInt("co") > 7)
                	returnValue="exist";
                else
                	returnValue="noExist";
            }
            pstmt.close();
            rs.close();
            
            pstmt2                = con.prepareStatement(checkSql);
            rs2                   = pstmt2.executeQuery();
            
            while(rs2.next()){
                //System.out.println("FRCST_DE       : " + rs2.getString("FRCST_DE"));
                frcstDayList.add(rs2.getString("FRCST_DE"));
            }               
            
            pstmt2.close();
            rs2.close();
    		
        } catch (SQLException e) { System.out.println("쿼리 수행 실패"); }  
        
        
        return returnValue;
    }
	
	public JSONObject apiCheck(String apiData){
		
		 try {
	            JSONParser jsonParser = new JSONParser();
	            JSONObject jsonObj = (JSONObject) jsonParser.parse(apiData);
	            String resultCode = "";
	            
	            JSONObject ds = (JSONObject) jsonObj.get("response");
	            
	            JSONObject result = (JSONObject) ds.get("header");
	            
	            System.out.println("resultCode : "+ result.get("resultCode"));
	            
	            resultCode = result.get("resultCode").toString();
	            
	            if (resultCode.equals("0000"))
	            {
	            	JSONObject body = (JSONObject) ds.get("body");
	            	JSONObject items = (JSONObject) body.get("items");
	            	JSONObject item = (JSONObject) items.get("item");
	            
	            	return item;
	            }

	        } catch (ParseException e) {
	            // TODO Auto-generated catch block
	            e.printStackTrace();
	        }
		 
		return null;		
		
	}
	
	public String dbInsert(JSONObject item){		
		String sql    = "insert into ST_VOC014_L (FRCST_DE, FRCST_ZONE_CODE, AM_WETHR, PM_WETHR, LWET_TMPRT, TOP_TMPRT, REGIST_DT, RESULT_CODE) values (?,?,?,?,?,?, now(),'0000') ";
		String upDatesql = "update ST_VOC014_L set FRCST_ZONE_CODE=?, AM_WETHR=?, PM_WETHR=?, LWET_TMPRT=?, TOP_TMPRT=?, REGIST_DT=now(), RESULT_CODE='0000' where FRCST_DE=?";
        
        try {
       		
        		for(int j=0; j < day.length; j++)
        		{        		
        			//System.out.println(frcstDayList.get(j));
        			
        			if (frcstDayList.contains(day[j].toString())) {
        				
        				pstmt = con.prepareStatement(upDatesql);            
		        		
		        		pstmt.setString(1, item.get("regId").toString());
		        		if (j < 5) {
		        			pstmt.setString(2, item.get("wf" + (j+3) + "Am").toString());
		        			pstmt.setString(3, item.get("wf" + (j+3) + "Pm").toString());
		        		} else {
		        			pstmt.setString(2, item.get("wf" + (j+3)).toString());
		        			pstmt.setString(3, item.get("wf" + (j+3)).toString());
		        		}
		        		pstmt.setLong(4, (long)item.get("taMin" + (j+3)));
		        		pstmt.setLong(5, (long)item.get("taMax" + (j+3)));
		        		pstmt.setString(6, day[j]);
        				
        			}else{
        				
		        		pstmt = con.prepareStatement(sql);            
		        		pstmt.setString(1, day[j]);
		        		pstmt.setString(2, item.get("regId").toString());
		        		if (j < 5) {
		        			pstmt.setString(3, item.get("wf" + (j+3) + "Am").toString());
		        			pstmt.setString(4, item.get("wf" + (j+3) + "Pm").toString());
		        		} else {
		        			pstmt.setString(3, item.get("wf" + (j+3)).toString());
		        			pstmt.setString(4, item.get("wf" + (j+3)).toString());
		        		}
		        		pstmt.setLong(5, (long)item.get("taMin" + (j+3)));
		        		pstmt.setLong(6, (long)item.get("taMax" + (j+3)));
		        		
        			}
	        		
	        		pstmt.executeUpdate();
        		}
        	
            pstmt.close();

        } catch (SQLException e) { System.out.println("쿼리 수행 실패" + e); }
        
        return "";
    }



	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		CallRestWS2 crw = new CallRestWS2();
		String ret = "";
		JSONObject item = null;
		
		String preCheckResult = "";
		
		// 01. 기존 수집 여부 체크
		preCheckResult = crw.preCheck();				
		
		// 02. OpenAPI 데이터 수신
		
		if (preCheckResult.equals("exist"))	// 수집이 완료되었다면 프로그램 종료
		{
			
			System.exit(0);
			
		} else {
			
			ret = crw.restClientMiddleLandWeather();
			System.out.println(ret);
			item = crw.apiCheck(XML.toJSONObject(ret).toString());			
			
			
			ret = crw.restClientMiddleTemperature();
			System.out.println(ret);
			item.putAll(crw.apiCheck(XML.toJSONObject(ret).toString()));			
		}	
				
		// 03. DB Insert 및 체크
		
		if (item == null){			// 수신된 데이터에 이상 있음
			System.exit(1);
		}else{								// DB Insert
			
			crw.dbInsert(item);
			
		}
		
		// 04. 종료
		crw.con.close();			
	}

}
