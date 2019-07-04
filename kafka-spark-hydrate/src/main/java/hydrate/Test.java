package hydrate;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

public class Test {
	private static final int numThreads = 4;
	private static String URL = "http://192.168.158.1:8080/demo/user";
	
	public Iterator calc(Iterator input) throws Exception {
		
		//long pid = getPID();
		//System.out.println(pid);
		//Thread.currentThread().sleep(20000);
		
		ExecutorService executor = Executors.newFixedThreadPool(numThreads);
		
		List <Future<String>> list = new ArrayList<Future<String>>();
		
		List <String> urls = new ArrayList<String>();

		/*for(int i=0;i<48;i++){
			urls[i]=URL+"/"+i;
		}*/
		while (input.hasNext()) {
            StringBuilder stringUrl = new StringBuilder();
            stringUrl.append(input.next());
            String url=stringUrl.toString();
            url=url.substring(1,url.length()-1);
            System.out.println(url);
        	urls.add(url);
        }
 
		long startTime = System.currentTimeMillis();

		for (int i = 0; i < urls.size(); i++) {
 
			String url = urls.get(i);
			Future<String> worker = executor.submit(new CallableUrl(url));
			list.add(worker);
			//executor.submit(worker);
		}
		executor.shutdown();
		List<String> result = new ArrayList<String>();
		for(Future<String> fut : list){
			try{
					//System.out.println(fut.get());
					result.add(fut.get());
			}
			catch(InterruptedException e){                                               
				e.printStackTrace();
			}
			/*catch (ExecutionException e) {
				e.printStackTrace();
			}*/
		}
		
		long endTime   = System.currentTimeMillis();
		double totalTime = (endTime - startTime)/1000.0;
		System.out.println(totalTime);               
		System.out.println("\nFinished all threads");
		
		return result.iterator();
	}
 
	public static class CallableUrl implements Callable {
		
		private final String url;
		 
		CallableUrl(String url) {
			this.url = url;
		}
 
		public String call() throws Exception{
 
			String result="";
			
			int code = 200;
			try {
				URL siteURL = new URL(url);
				HttpURLConnection connection = (HttpURLConnection) siteURL.openConnection();
				connection.setRequestMethod("GET");
				connection.setConnectTimeout(3000);
				connection.connect();
 
				code = connection.getResponseCode();
				if (code == 200) {
					Scanner sc = new Scanner(siteURL.openStream());
					String inline="";
					while(sc.hasNext())
					{
						inline+=sc.nextLine();
					}
					//System.out.println("\nJSON data in string format");
					//System.out.println(inline);
					result=inline;
					sc.close();
				} else {
					result = "-> Yellow <-\t" + "Code: " + code;
				}
				connection.disconnect();
			} catch (Exception e) {
				result = "-> Red <-\t" + "Wrong domain - Exception: " + e.getMessage();
 
			}
			return (/*url + "\t\tStatus:" + */result);
		}
	}
	
	/*public static long getPID() {
	    String processName = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
	    if (processName != null && processName.length() > 0) {
	        try {
	            return Long.parseLong(processName.split("@")[0]);
	        }
	        catch (Exception e) {
	            return 0;
	        }
	    }
	    return 0;
	}*/
}
