package cs5540.bigdata.project;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.scribe.builder.*;
import org.scribe.builder.api.*;
import org.scribe.model.*;
import org.scribe.oauth.*;
import org.scribe.oauth.OAuthService;

public class TwitterStreamConsumer extends Thread {
	private static final String STREAM_URI = "https://stream.twitter.com/1.1/statuses/filter.json";

	public void run(){
		try{
			System.out.println("Starting twitter public stream...");

			// 1. Enter consumer key and secret
			OAuthService service = new ServiceBuilder()
				.provider(TwitterApi.class)
				.apiKey("DsrKVsGSct5khALPJJh3VM7aZ")
				.apiSecret("SCrcpDiw5Pw7mKqBBn6f4cLw2I4Fhxss1mI8eWLp5kRJy9LWcP")
				.build();

			// 2. Set access token (taken from twitter page)
			Token accessToken = new Token("3633627554-rFls2gI4qbrqJTbR0BZKoCADouY2TkjLFEa8FQe", "GBgogkDScpUknkuXrl4nNrQhEPIW65TJlkRkX3rkBHqO2");

			// 3. Generate streaming request
			OAuthRequest request = new OAuthRequest(Verb.POST, STREAM_URI);
			request.addHeader("version", "HTTP/1.1");
			request.addHeader("host", "stream.twitter.com");
			request.addHeader("user-agent", "Twitter Stream Reader");
			request.addBodyParameter("track", "java,heroku,twitter");
			request.setConnectionKeepAlive(true);
			service.signRequest(accessToken, request);
			
			Response response = request.send();
			
			// 4. Create reader to read streamed data
			BufferedReader reader = new BufferedReader(
				new InputStreamReader(response.getStream())
				);

			String line;
			while((line = reader.readLine()) != null){
				System.out.println(line);
			}
		}catch(IOException e){
			e.printStackTrace();
		}
	}
}
