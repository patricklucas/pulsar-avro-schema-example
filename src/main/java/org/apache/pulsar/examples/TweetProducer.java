package org.apache.pulsar.examples;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.schema.AvroSchema;

/**
 * Producer to produce tweets in Avro format.
 */
public class TweetProducer {

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: TweetProducer [service-url]");
            return;
        }
        String serviceUrl = args[0];

        try (PulsarClient client = PulsarClient.builder()
            .serviceUrl(serviceUrl)
            .build()) {
            try (Producer<TwitterSchema> producer = client.newProducer(AvroSchema.of(TwitterSchema.class))
                 .topic("twitter-avro")
                 .create()) {
                for (int i = 0; i < 10; i++) {
                    Fixed fixed = new Fixed("Hello".getBytes(StandardCharsets.UTF_8));
                    TwitterSchema tweet = new TwitterSchema("user-" + i, "tweet-" + i, fixed, System.currentTimeMillis());

                    DatumWriter<TwitterSchema> writer = new SpecificDatumWriter<>(TwitterSchema.class);
                    ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    Encoder encoder = EncoderFactory.get().jsonEncoder(TwitterSchema.getClassSchema(), bos, true);
                    writer.write(tweet, encoder);
                    encoder.flush();
                    System.out.println("Tweet JSON: " + bos.toString("UTF-8"));

                    producer.send(tweet);
                }
                System.out.println("Successfully produce 10 tweets");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
