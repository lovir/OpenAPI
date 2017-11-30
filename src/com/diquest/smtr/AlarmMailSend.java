package com.diquest.smtr;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;

import com.diquest.ir.client.command.CommandSearchRequest;
import com.diquest.ir.common.exception.IRException;
import com.diquest.ir.common.msg.protocol.Protocol;
import com.diquest.ir.common.msg.protocol.query.FilterSet;
import com.diquest.ir.common.msg.protocol.query.GroupBySet;
import com.diquest.ir.common.msg.protocol.query.Query;
import com.diquest.ir.common.msg.protocol.query.QuerySet;
import com.diquest.ir.common.msg.protocol.query.SelectSet;
import com.diquest.ir.common.msg.protocol.query.WhereSet;
import com.diquest.ir.common.msg.protocol.result.GroupResult;
import com.diquest.ir.common.msg.protocol.result.Result;
import com.diquest.ir.common.msg.protocol.result.ResultSet;

import java.util.Properties;
import javax.mail.Authenticator;
import javax.mail.Message;
import javax.mail.Multipart;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;


public class AlarmMailSend {
	
	String driver        = "org.mariadb.jdbc.Driver";
    String url           = "jdbc:mariadb://172.22.11.16:3307/voc";
    String uId           = "diquest";
    String uPwd          = "ek2znptm2!@";
    //어제 날짜 08시
    String lastDay 		= "";
    String lastDay2		= ""; 
    
    Connection               con;
    PreparedStatement        pstmt;
    java.sql.ResultSet       rs;
    
    String [] alarmKeyword = new String[1000];
    int [] alarmLevel1 = new int[1000];
	int [] alarmLevel2 = new int[1000];
	int [] alarmLevel3 = new int[1000];
	String [] alarmMailReceiver = new String[1000];
	
	public AlarmMailSend(){
		
		Calendar cal = new GregorianCalendar();
    	cal.add(Calendar.DATE, -1);
    	SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
    	
    	lastDay = sdf.format(cal.getTime());
    	cal.add(Calendar.DATE, -1);
    	lastDay2 = sdf.format(cal.getTime());
    	
    	//System.out.println("lastDay ::: " + lastDay);
    	//System.out.println("lastDay2 ::: " + lastDay2);
  			
        try {
           Class.forName(driver);
           con = DriverManager.getConnection(url, uId, uPwd);
           
           if( con != null ){ System.out.println("데이터 베이스 접속 성공"); }
           
       } catch (ClassNotFoundException e) { System.out.println("드라이버 로드 실패");    } 
         catch (SQLException e) { System.out.println("데이터 베이스 접속 실패"); }
		
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
//		String [] alarmKeywords = new String [5];
//		alarmKeywords[0] = "수능";
//		alarmKeywords[1] = "연착";
//		alarmKeywords[2] = "노숙자";
//		alarmKeywords[3] = "잡상인";
//		alarmKeywords[4] = "사고";
//		
//		int alarmLevel1 = 170;
//		int alarmLevel2 = 100;
//		int alarmLevel3 = 50;
//		
//		String alarmMailReceiver = "shhwang@diquest.com";		
			
		AlarmMailSend ams = new AlarmMailSend();
		
		ams.preCheck();

		for (int i = 0; i < ams.alarmKeyword.length && ams.alarmKeyword[i] != null && !ams.alarmKeyword[i].equals(""); i++){
			ams.alarmCheck(ams.alarmKeyword[i], ams.alarmLevel1[i], ams.alarmLevel2[i], ams.alarmLevel3[i], ams.alarmMailReceiver[i]);
		}
	}

	private void alarmCheck(String alarmKeywords, int alarmLevel1, int alarmLevel2, int alarmLevel3, String alarmMailReceiver) throws Exception {
		// TODO Auto-generated method stub
		
		float raiseRate = 0;
		Calendar calnow = new GregorianCalendar();
		
		raiseRate = getRaiseRate(alarmKeywords);
		
		//System.out.println(alarmKeywords + "===" + raiseRate);
		
		if (raiseRate > alarmLevel1) {
			System.out.println(calnow.getTime().toString() + " : " + alarmMailReceiver + " >> 발생 레벨 - 상 등급 메일 발송");
			sendAlarmMail(alarmMailReceiver, alarmKeywords, raiseRate, "발생 레벨 - 상 등급");		
		} else if (raiseRate > alarmLevel2) {
			System.out.println(calnow.getTime().toString() + " : " +alarmMailReceiver + " >> 발생 레벨 - 중 등급 메일 발송");
			sendAlarmMail(alarmMailReceiver, alarmKeywords, raiseRate, "발생 레벨 - 중 등급");			
		} else if (raiseRate > alarmLevel3) {
			System.out.println(calnow.getTime().toString() + " : " +alarmMailReceiver + " >> 발생 레벨  - 하 등급 메일 발송");
			sendAlarmMail(alarmMailReceiver, alarmKeywords, raiseRate, "발생 레벨  - 하 등급");			
		}
		
	}

	private float getRaiseRate(String searchTerm) throws IRException {
		// TODO Auto-generated method stub
		
		String adminIP = "172.22.11.16"; // 서버 IP
		int adminPORT = 5555; // 서버 PORT
		String collectionName = "METRO_VOC"; // 검색 대상 컬렉션

		char[] startTag = "<b><u>".toCharArray(); // Highlight tag 설정 startTag
		char[] endTag = "</u></b>".toCharArray(); // Highlight tag 설정 endTag
		Query query = new Query(startTag, endTag); // 전송하기 위한 Query를 설정합니다.
		QuerySet querySet = new QuerySet(1); // Query를 담기 위한 QuerySet 설정
		
		Result result = null;
		Result[] resultlist = null;
		
		query.setLoggable(false); // 검색 로그 설정(FullLog)
		query.setDebug(true); // Debug 설정
		query.setPrintQuery(false); // PrintQuery 설정
		query.setFrom(collectionName); // From 설정, 검색할 컬렉션을 선택
		query.setResult(0, 0);
			
		//int totalCount=0;
		
		SelectSet[] selectSet = null;
		WhereSet[] whereSet = null;
		ArrayList<WhereSet> whereSetList = new ArrayList<WhereSet>();
	
		selectSet = new SelectSet[] {
				new SelectSet("DAY", Protocol.SelectSet.NONE)
		};
		
		whereSetList.add(new WhereSet("KEYWORD", Protocol.WhereSet.OP_HASALL, searchTerm, 1));
		whereSet = new WhereSet[whereSetList.size()];
		for(int i = 0 ; i < whereSetList.size() ; i++) {
			whereSet[i] = (WhereSet) whereSetList.get(i);
		}
		
		String[] filterKeywords = {lastDay, lastDay2};
		FilterSet[] filterSets = new FilterSet[] {
		new FilterSet(Protocol.FilterSet.OP_MATCH, "DAY", filterKeywords, 1)
		};
		
		GroupBySet[] groupBys = new GroupBySet[]{
				new GroupBySet("DAY", (byte) (Protocol.GroupBySet.OP_COUNT | Protocol.GroupBySet.ORDER_NAME), "ASC", "")
		};
		
		query.setSelect(selectSet);
		query.setWhere(whereSet);
		query.setFilter(filterSets);
		//query.setOrderby(orderbys);
		query.setGroupBy(groupBys);
		querySet.addQuery(query);
		
		CommandSearchRequest command = new CommandSearchRequest(adminIP, adminPORT);
		
		int returnCode = command.request(querySet);
		if (returnCode > 0) {
			ResultSet results = command.getResultSet();
			resultlist = results.getResultList();
		}else{
			resultlist = new Result[1];
			resultlist[0] = new Result();
		}
		
		int groupCount[] = new int[2];

		for (int k = 0; resultlist != null && k < resultlist.length; k++) {
			
			result = resultlist[k];
			
			if (result != null && result.getRealSize() != 0) {
				//System.out.println("<br><b>Total Count</b> : <u>" + result.getTotalSize() + "</u>");
				//out.println("<hr>");
				if (result.getGroupResultSize() != 0) {
					GroupResult[] groupResults = result.getGroupResults();
					
					for (int i = 0; i < groupResults.length; i++) {
						int rSize = groupResults[i].groupResultSize();
						
						for (int j = 0; j < rSize; j++) {
							String id = new String(groupResults[i].getId(j));
							int value = groupResults[i].getIntValue(j);
							
							groupCount[j] = value;
							
							//System.out.println(id + " [<u>" + value + "</u>] <br>");
						}
						//out.println("<br>");
					}
				}
				
			} else {
				return 0;
			}
		}
		
		//System.out.println( "groupCount[0] ::: " + groupCount[0]);
		//System.out.println( "groupCount[1] ::: " + groupCount[1]); 
		//System.out.println( "(groupCount[1] - groupCount[0]) / groupCount[0] * 100 ==== " + (float)(groupCount[1] - groupCount[0]) / groupCount[0] * 100 ) ;
		
		if (groupCount[0] == 0) groupCount[0] = 1;
		
		return ( (float)(groupCount[1] - groupCount[0]) / groupCount[0] * 100);
	}
	
	public void sendAlarmMail(String email, String keyword, float raseRate, String level ) throws Exception{
	    
        Properties props = new Properties(); 
        props.setProperty("mail.transport.protocol", "smtp"); 
        props.setProperty("mail.host", "mail.seoulmetro.co.kr"); 
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.port", "25");
        props.put("mail.smtp.socketFactory.port", "25");
        //props.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory"); 
        props.put("mail.smtp.socketFactory.fallback", "false"); 
        props.setProperty("mail.smtp.quitwait", "false"); 
         
        Authenticator auth = new Authenticator(){
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication("bigdata@seoulmetro.co.kr", "bigtata*1"); 
            }
        };
    
        Session session = Session.getDefaultInstance(props,auth);
         
        MimeMessage message = new MimeMessage(session); 
        message.setSender(new InternetAddress("bigdata@seoulmetro.co.kr")); 
        message.setSubject("[민원 분석 시스템] 알람 키워드 (" + keyword + ") 메일"); 
 
        message.setRecipient(Message.RecipientType.TO, new InternetAddress(email)); 
        
        String mailContent = "";
        mailContent = "[민원분석 시스템] 알람 키워드  (" + keyword + ") 메일 \n\n\n";
        mailContent += "알람 키워드  '" + keyword + "'이(가) 전일 대비 발생 건수가 " + raseRate + "% 증가하여 [ " + level + "] 메일을 발송합니다.\n\n";
        mailContent += "알람 키워드 '" + keyword + "'의 상세내용은 빅데이터 분석시스템의 민원 분석 시스템에서 확인하시기 바랍니다.";
         
        Multipart mp = new MimeMultipart();
        MimeBodyPart mbp1 = new MimeBodyPart();
        mbp1.setText(mailContent);
        mp.addBodyPart(mbp1);
         
        message.setContent(mp);
         
        Transport.send(message);
    }
	
	public String preCheck(){
			
        String sql    = "select a.KEYWORD, b.LEVEL_1, b.LEVEL_2, b.LEVEL_3, c.EMAIL "
        				+ " from CUT_ALM_KEYWORD a, CUT_ALM_MANAGE b, CUT_ALM_MAIL c "
        				+ " where a.REG_ID = b.REG_ID and a.REG_ID = c.REG_ID"
        				+ " and a.USE_YN = 'Y' and c.REG_YN = 'Y'";
        try {
            pstmt                = con.prepareStatement(sql);
            rs                   = pstmt.executeQuery();
            int i = 0;
            
//            alarmKeyword = new String[rs.getRow()];
//            alarmLevel1 = new int[rs.getRow()];
//            alarmLevel2 = new int[rs.getRow()];
//            alarmLevel3 = new int[rs.getRow()];
//            alarmMailReceiver = new String[rs.getRow()];
            
            while(rs.next()){
                //System.out.println("KEYWORD       : " + rs.getString("KEYWORD"));
                //System.out.println("LEVEL_1       : " + rs.getInt("LEVEL_1"));
                //System.out.println("EMAIL       : " + rs.getString("EMAIL"));
                
                alarmKeyword[i] = rs.getString("KEYWORD");
                alarmLevel1[i] = rs.getInt("LEVEL_1");
                alarmLevel2[i] = rs.getInt("LEVEL_2");
                alarmLevel3[i] = rs.getInt("LEVEL_3");
                alarmMailReceiver[i] = rs.getString("EMAIL");
                                		
           		i++;
            }
            
            rs.close();
            pstmt.close();
    		
        } catch (SQLException e) { System.out.println("쿼리 수행 실패"); }  
        
        return "noExist";
    }

}
