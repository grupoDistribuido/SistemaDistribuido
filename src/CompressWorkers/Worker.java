package CompressWorkers;

import MessageDef.MessageImplementation;
import MessageDef.MessageImplementation.MessageJob;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Worker {

    public static void main(String args[]){

        ExecutorService executorService = Executors.newFixedThreadPool(20);
        // Aqui se recibe un mensaje de la cola, se lo deserializa y se lo manda a un thread.
        try {

            FileInputStream fileInput = new FileInputStream("/home/jmurillo/mensaje");

            executorService.execute(new zipper(MessageJob.parseFrom(fileInput)));


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
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
                    socket = new Socket(this.job.getInfo().getIP(), this.job.getInfo().getPort());
                    OutputStreamWriter out = new OutputStreamWriter(socket.getOutputStream(),"UTF-8");

                    origin_file = new File(this.job.getOrigin());
                    destiny_file = new File(this.job.getDestiny());

                    ZipFile zip = new ZipFile(destiny_file);


                    zip.createZipFile(origin_file, zp);

                    System.out.println("Zipfile done!!!!!!! :D");
                    response = String.format("File succesfully created at %s . :3" , destiny_file);
                    out.write(response);
                } catch (IOException e) {
                    e.printStackTrace();
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
