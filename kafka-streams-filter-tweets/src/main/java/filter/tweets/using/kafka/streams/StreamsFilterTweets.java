package filter.tweets.using.kafka.streams;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamsFilterTweets {

        public static void main(String[] args) {
                //create properties
                Properties properties = new Properties();
                properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
                properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
                properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
                properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
                //These two properties mean that we will have strings as keys and strings as values


                //create a topology
                StreamsBuilder streamsBuilder = new StreamsBuilder();
                KStream<String, String> inputTopic = streamsBuilder.stream("twitter_tweets");
                KStream<String, String> filteredStream = inputTopic.filter(
                        (k, jsonTweet)-> extractUserFollowersInTweet(jsonTweet) > 10000
                        //filter for tweets which has over 10000 followers
                        );
                filteredStream.to("important_tweets");

                //build the topology
                KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);

                //start our streams application
                kafkaStreams.start();
        }

        private static JsonParser jsonParser = new JsonParser();
        private static int extractUserFollowersInTweet(String tweetJson) {
                //gson library
                try{
                     return jsonParser.parse(tweetJson).getAsJsonObject().get("user").getAsJsonObject().get("followers_count").getAsInt();
                }catch(NullPointerException e) {
                        return 0;
                }
        }
}
