package com.github.kafka.learning.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducerStock {
    private static final Logger logger = LoggerFactory.getLogger(TwitterProducerStock.class);

    private static final String API_KEY = "SbXF2DNIKxac0YpUW2FhhoVhg";
    private static final String API_SECRET = "86hgKV0XkYLLOtLS298xEFj8Bbeaob6Dsccm7mS6Z3oMO9oFU1";
    private static final String BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAAGE0MQEAAAAAhw6ugLKqa9ujYIz9%2Bqo%2BmBssytI%3D4rVqi3E91gKbhBzy1Rn8d6J3tFQ10LMWaWD0aY4YBsGTGATFcf";
    private static final String ACCESS_TOKEN = "51990324-I3bJA1nIsQUvdvI4QYM1W5VY2dQMh09xDYTZPF3yC";
    private static final String ACCESS_TOKEN_SECRET = "cDMtRbuvavdEnAnu0gwrXNXmRat4RN8P3uYo8r8EJ79fu";

    public TwitterProducerStock(){

    }

    public void run(){
        //create a twitter client
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */

        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);
        Client twitterClient = createTwitterClient(msgQueue);
        twitterClient.connect();

        //create a kafka producer
        //loop to send tweets to kafka
        // on a different thread, or multiple different threads....
        HashMap<String, Integer> stockTweetCount = new HashMap<>();
        initializeStockTweetCount(stockTweetCount);
        while (!twitterClient.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                twitterClient.stop();
            }
            if(msg != null){
                for (String sym : stockTweetCount.keySet()) {
                    if(msg.contains(sym)){
                        stockTweetCount.put(sym, stockTweetCount.get(sym) + 1);
                    }
                }
                logger.info(msg);
                displayTweetSummaryCount(stockTweetCount);
            }
        }

        logger.info("End of application");
    }

    private void displayTweetSummaryCount(HashMap<String, Integer> stockTweetCount) {
        Iterator<String> itr = stockTweetCount.keySet().iterator();
        while (itr.hasNext())
        {
            String key = itr.next();
            int value = stockTweetCount.get(key);

            System.out.println(key + "=" + value);
        }
    }

    private void initializeStockTweetCount(HashMap<String, Integer> stockTweetCount) {
        stockTweetCount.put("APPL", 0);
        stockTweetCount.put("FB", 0);
        stockTweetCount.put("GME", 0);
        stockTweetCount.put("NKLA", 0);
        stockTweetCount.put("NOK", 0);
    }

    public Client createTwitterClient(final  BlockingQueue<String> msgQueue){

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
       // List<Long> followings = Lists.newArrayList(1234L, 566788L);
        List<String> terms = Lists.newArrayList("GME", "NOK", "APPL", "FB", "NKLA");
       // hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(API_KEY, API_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        return hosebirdClient;
    }

    public static void main(String[] args) {
        new TwitterProducerStock().run();
    }
}
