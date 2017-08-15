package CompressClients;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

public class Client {

    public static void main(String[] args){
        String IP = args[0];
        int port = Integer.parseInt(args[1]);

        Thread listenerThread = new Thread(new responseListener(IP, port));

        listenerThread.start();

        AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider().getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Can't load the credentials from the credential profiles file. " +
                            "Please make sure that your credentials file is at the correct " +
                            "location (~/.aws/credentials), and is a in valid format.",
                    e);
        }

        AmazonSQSClient sqs = new AmazonSQSClient(credentials);
        sqs.setEndpoint("https://sqs.us-east-2.amazonaws.com");


        try {

            // Create a FIFO queue
            /*System.out.println("Creating a new Amazon SQS FIFO queue called MyFifoQueue.fifo.\n");
            Map<String, String> attributes = new HashMap<String, String>();
            // A FIFO queue must have the FifoQueue attribute set to True
            attributes.put("FifoQueue", "true");
            // Generate a MessageDeduplicationId based on the content, if the user doesn't provide a MessageDeduplicationId
            attributes.put("ContentBasedDeduplication", "true");
            // The FIFO queue name must end with the .fifo suffix
            CreateQueueRequest createQueueRequest = new CreateQueueRequest("MyFifoQueue.fifo").withAttributes(attributes);
            String myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();*/

            // List queues
            System.out.println("Listing all queues in your account.\n");
            for (String queueUrl : sqs.listQueues().getQueueUrls()) {
                System.out.println("  QueueUrl: " + queueUrl);
            }
            System.out.println();

            // Send a message
            String myQueueUrl = "https://sqs.us-east-2.amazonaws.com/364319865579/altaPrioridad.fifo";
            System.out.println("Sending a message to MyFifoQueue.fifo.\n");

            SendMessageRequest sendMessageRequest = new SendMessageRequest(myQueueUrl, "This is my message text.");
            // You must provide a non-empty MessageGroupId when sending messages to a FIFO queue
            sendMessageRequest.setMessageGroupId("messageGroup1");
            // Uncomment the following to provide the MessageDeduplicationId
            sendMessageRequest.setMessageDeduplicationId("1");

            //Cargo Archivo

            byte[] archivo = Files.readAllBytes(new File("/home/jmurillo/yoshi.png").toPath());

            Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
            //messageAttributes.put("Image", new MessageAttributeValue().withDataType("Binary").withBinaryValue(ByteBuffer.wrap(archivo)));
            messageAttributes.put("Image1", new MessageAttributeValue().withDataType("String").withStringValue("HEEEEY"));

            for (Map.Entry<String, MessageAttributeValue> entry : messageAttributes.entrySet()) {
                System.out.println("  Attribute");
                System.out.println("    Name:  " + entry.getKey());
                System.out.println("    Value: " + entry.getValue().toString());
            }


            sendMessageRequest = sendMessageRequest.withMessageAttributes(messageAttributes);
            //sendMessageRequest = sendMessageRequest.addMessageAttributesEntry();


            SendMessageResult sendMessageResult = sqs.sendMessage(sendMessageRequest);
            String sequenceNumber = sendMessageResult.getSequenceNumber();
            String messageId = sendMessageResult.getMessageId();
            System.out.println("SendMessage succeed with messageId " + messageId + ", sequence number " + sequenceNumber + "\n");










        } catch (IOException e) {
            e.printStackTrace();
        }

    }


}

class responseListener implements Runnable {

    String IP;
    int port;
    String response;

    public responseListener(String IP, int port) {
        this.IP = IP;
        this.port = port;
    }

    @Override
    public void run() {
        try {
            ServerSocket socket = new ServerSocket(this.port);
            Socket client = socket.accept();


            InputStreamReader input = new InputStreamReader(client.getInputStream());
            BufferedReader reader = new BufferedReader(input);
            while (true) {
                response = String.format("Response: %s", reader.readLine());
                System.out.println(response);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
