package cs5540.bigdata.project;

public class TwitterStreamServer {
	public static void main(String[] args){
		final TwitterStreamConsumer streamConsumer = 
			new TwitterStreamConsumer();
		streamConsumer.start();
	}
}
