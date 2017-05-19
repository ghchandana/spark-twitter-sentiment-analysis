package org.srp.spark.twitter.sentiment.analysis;

import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.util.CoreMap;
import scala.Tuple2;
import twitter4j.Status;

public class TwitterAnalysisJava {

	public static void main(String[] args) throws InterruptedException {
		System.setProperty("twitter4j.oauth.consumerKey", "<consumerKey>");
	    System.setProperty("twitter4j.oauth.consumerSecret", "<consumerSecret>");
	    System.setProperty("twitter4j.oauth.accessToken", "<accessToken>");
	    System.setProperty("twitter4j.oauth.accessTokenSecret", "<accessTokenSecret>");
		
		final SparkConf sparkConf = new SparkConf().setAppName("twitter-streaming-analysis").setMaster("local[2]");
		final JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, new Duration(1000));
		streamingContext.sparkContext().setLogLevel("OFF");

		JavaReceiverInputDStream<Status> tweets = TwitterUtils.createStream(streamingContext, new String[] { "India" });
		tweets.map(status -> status.getText()).map(tweet -> new Tuple2<String, String>(tweet, sentiment(tweet)))
				.foreachRDD(rdd -> rdd.collect()
						.forEach(tuple -> System.out.println(" Sentiment => " + tuple._2 + " :-: TWEET => " + tuple._1)));

		streamingContext.start();
		streamingContext.awaitTermination();

	}

	public static String sentiment(String tweets) {
		Integer mainSentiment = 0, longest = 0;
		final String[] sentimentText = { "Very Negative", "Negative", "Neutral", "Positive", "Very Positive" };
		final Properties props = new Properties();
		props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
		for (CoreMap sentence : new StanfordCoreNLP(props).process(tweets).get(CoreAnnotations.SentencesAnnotation.class)) {
			Integer sentiment = RNNCoreAnnotations.getPredictedClass(sentence.get(SentimentCoreAnnotations.AnnotatedTree.class));
			final String partText = sentence.toString();
			if (partText.length() > longest) {
				mainSentiment = sentiment;
				longest = partText.length();
			}
		}
		return sentimentText[mainSentiment];
	}

}
