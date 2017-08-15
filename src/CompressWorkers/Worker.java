package CompressWorkers;

import MessageDef.MessageImplementation;
import MessageDef.MessageImplementation.MessageJob;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import net.lingala.zip4j.exception.ZipException;
import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.model.ZipParameters;
import sun.misc.resources.Messages;

import java.io.*;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Worker {

    public static void main(String args[]){

        ExecutorService executorService = Executors.newFixedThreadPool(20);
        // Aqui se recibe un mensaje de la cola, se lo deserializa y se lo manda a un thread.


        AWSCredentials credentials = null;
        String myQueueUrl = "https://sqs.us-east-2.amazonaws.com/364319865579/altaPrioridad.fifo";
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

            for (String queueUrl : sqs.listQueues().getQueueUrls()) {
                System.out.println("  QueueUrl: " + queueUrl);
            }
            System.out.println();

            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrl);
            //ArrayList<String> atributos = new ArrayList<>();
            //atributos.add("All");
            receiveMessageRequest = receiveMessageRequest.withAttributeNames("All");//  setAttributeNames(atributos);
            // Uncomment the following to provide the ReceiveRequestDeduplicationId
            //receiveMessageRequest.setReceiveRequestAttemptId("1");
            List<com.amazonaws.services.sqs.model.Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
            for (com.amazonaws.services.sqs.model.Message message : messages) {
                System.out.println("  Message");
                System.out.println("    MessageId:     " + message.getMessageId());
                System.out.println("    ReceiptHandle: " + message.getReceiptHandle());
                System.out.println("    MD5OfBody:     " + message.getMD5OfBody());
                System.out.println("    Body:          " + message.getBody());

                String messageBody = message.getBody();
                String[] param = messageBody.split(";");
                MessageJob job = MessageJob.newBuilder().setOrigin(param[0]).
                        setDestiny(param[1]).setInfo(MessageJob.clientInfo.newBuilder().setIP(param[2])
                        .setPort(Integer.parseInt(param[3])).build()).setPrior(MessageJob.Priority.HIGH).setOp(MessageJob.Operation.CREATE).build();

                executorService.execute(new zipper(job));
            }



            System.out.println();
        }catch(AmazonServiceException e){


        }









    }



}



class zipper implements Runnable{

    MessageJob job;


    public zipper (MessageDef.MessageImplementation.MessageJob Job)  {

            this.job = Job;

    }

    public void run(){

        File origin_file ;
        File destiny_file ;

        Socket socket;
        ZipParameters zp;

        String response;

        switch (this.job.getOp()){

            case CREATE:

                zp = new ZipParameters();


                try {
                    //Start the socket for responses
                    //socket = new Socket(this.job.getInfo().getIP(), this.job.getInfo().getPort());
                    //OutputStreamWriter out = new OutputStreamWriter(socket.getOutputStream(),"UTF-8");

                    origin_file = new File(this.job.getOrigin());
                    destiny_file = new File(this.job.getDestiny());

                    ZipFile zip = new ZipFile(destiny_file);


                    zip.createZipFile(origin_file, zp);

                    System.out.println("Zipfile done!!!!!!! :D");
                    response = String.format("File succesfully created at %s . :3" , destiny_file);
                    //out.write(response);
                } catch(ZipException ze){
                    ze.printStackTrace();
                }

                // This is the file to be zipped

                break;

            case CANCEL:

                destiny_file = new File(this.job.getDestiny());

                //erase file

                try {

                    socket = new Socket(this.job.getInfo().getIP(), this.job.getInfo().getPort());
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());


                    int cnt = 0;
                    while(!Files.deleteIfExists(destiny_file.toPath())){
                        Thread.sleep(500);
                        if(++cnt==10){
                            break;
                        }
                    }



                    if(cnt==10){
                        response = String.format("The job couldn't be canceled :(");
                        out.writeBytes(response);
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                break;

            case READ:
                destiny_file = new File(this.job.getDestiny());

                long size = destiny_file.length();

                try {
                    socket = new Socket(this.job.getInfo().getIP(), this.job.getInfo().getPort());
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());

                    response = String.format("The size of the compressed file is: %d bytes",size);

                    out.writeBytes(response);

                } catch (IOException e) {
                    e.printStackTrace();
                }


                break;

        }





    }


}
