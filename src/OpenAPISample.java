import java.io.InputStream;
import java.net.URL;
import java.net.URLEncoder;
import org.apache.cxf.helpers.IOUtils;
import org.apache.cxf.io.CachedOutputStream;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class OpenAPISample {
	
	// 기상 관측 데이터 OpenAPI
		private String restClientAutocat(String content) throws Exception{
			String addr = "http://172.22.11.16:8080/gs-rest-service-0.1.0/AutocatRequest?";
			String parameter = "content=";
			
			content = URLEncoder.encode(content, "UTF-8");
			
			addr = addr + parameter + content;
			
			System.out.println(addr);
			
			URL url = new URL(addr);
			InputStream in = url.openStream(); 
			CachedOutputStream bos = new CachedOutputStream();
			IOUtils.copy(in, bos);
			in.close();
			bos.close();
			return bos.getOut().toString();
		}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		OpenAPISample crw = new OpenAPISample();
		String ret = "";
		String content = "아래 건의 뜬걸 봤는데 아침에 출근길에 냉방을 틀어달라고,, 근데 저는 오히려 너무 추워서 이글을 남김니다,, 저는 매일 거의 같은 시간으로 5시50분에서 6시쯤에 잠실역을 이용하는데 너무 추워서 1시간 되는 출근길이 너무 곤혹입니다,, 1시간이나 되는 거리라서 조금이라도 눈을 붙일려고 하면 너무 추워서 1시간내내 몸을 웅크리고 있습니다,, 출근시간이 새벽이고 사람도 없어서 더 그런것 같네여,,, 너무 덥다고 건의 쓰신분은 붐비는 시간대를 말씀하시는것 같네요,,, 그리고 여름에도 새벽엔 냉방을 너무 강하게 틀더군요,, 습기 제거 때문에 그러는지 사람도 별로 없는 지하철에서 냉기가 가득해서 몸이 시려울정도더군요,, 왠만하면 정말 지하철이 타기 싫습니다,,, 5호선이랑 많이 비교 되는것 같습니다,,,    ";
		
		
		try {
			
			ret = crw.restClientAutocat(content);

            JSONParser jsonParser = new JSONParser();
            JSONObject jsonObj = (JSONObject) jsonParser.parse(ret);
            
            JSONObject body = (JSONObject) jsonObj.get("body");
            
        	JSONArray catArray = (JSONArray) body.get("autoCatList");
        
            System.out.println("=====category====");
            for(int i=0 ; i<catArray.size() ; i++){
                JSONObject tempObj = (JSONObject) catArray.get(i);
                System.out.println("rank : "+tempObj.get("rank"));
                System.out.println("categoryCode : "+tempObj.get("categoryCode"));
                System.out.println("cateogryName : "+tempObj.get("categoryName"));
                System.out.println("recommendScore : "+tempObj.get("recommendScore"));
               System.out.println("----------------------------");
            }          

        } catch (ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
		
	}

}
