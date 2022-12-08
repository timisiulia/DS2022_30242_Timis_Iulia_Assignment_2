package org.example;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

import java.io.*;
import java.util.Scanner;

public class NewTask {
    // will schedule tasks to our work queue
    private static final String TASK_QUEUE_NAME = "task_queue";

    public static void send() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername("kuzqlffd");
        factory.setPassword("51pIHogSfqbmgcSmh_7pMPVEOflUHze0");
        factory.setVirtualHost("kuzqlffd");
        factory.setHost("goose-01.rmq2.cloudamqp.com");
        factory.setConnectionTimeout(30000);
        factory.setRequestedHeartbeat(30);
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            //the queue will survive a RabbitMQ node restart. In order to do so, we need to declare it as durable
            //make sure that messages aren't lost:
            channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);

            CSVReader.read("src/main/java/org/example/sensor.csv", channel);


        }


    }
    public static class CSVReader {
        public static final String delimiter = ",";

        public static void read(String csvFile,Channel channel) {
            try {
                File file = new File(csvFile);
                FileReader fr = new FileReader(file);
                BufferedReader br = new BufferedReader(fr);
                String line = " ";
                String splitBy = ",";
                String[] tempArr;
                while ((line = br.readLine()) != null) {
                    String[] message = line.split(splitBy);    // use comma as separator
                channel.basicPublish("", TASK_QUEUE_NAME, null, message[0].getBytes());
                System.out.println(" [x] Sent '" + message[0] + "'");

                Thread.sleep(1000);
                }
                br.close();
            } catch (IOException ioe) {
                ioe.printStackTrace();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
    public static void main(String[] args) throws Exception {
        send();

    }

}
