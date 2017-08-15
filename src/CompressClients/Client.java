/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package consolajob;

import MessageDef.MessageImplementation;
import java.util.Scanner;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;

//Protocolo Buffer
import MessageDef.MessageImplementation.MessageJob;

//Amazon
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;

/**
 *
 * @author opozo
 */
public class ConsolaJob {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws UnknownHostException {
        
        //Variables que ingresa
        String[] comando;
        String delimiter = " ";
        String colaAlta="https://sqs.us-east-2.amazonaws.com/364319865579/altaPrioridad.fifo";
        String colaBaja="https://sqs.us-east-2.amazonaws.com/364319865579/bajaPrioridad.fifo";
        
        List<String> messagelist = new ArrayList<>();
        
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
        
        System.out.println("****** Bienvenido a la Consola de Trabajo *******");
        System.out.println();
        System.out.println("****** Si Desea Salir de la consola ponga salir()  *******");  
        
        InetAddress address = InetAddress.getLocalHost();
        String ip = address.getHostAddress();
        Integer puerto = 5000;
        
        while(true)
        {
            // TODO code application logic here     
            Scanner lector = new Scanner(System.in);
            //System.out.println("Ingrese el comando:");            
            String nombre = lector.nextLine();
            //lector.close();                      
        
            comando = nombre.split(delimiter);
            
            if(comando[0].equals("salir()")){ break;}
            else{
               if(comando[0].toUpperCase().equals("createJob".toUpperCase())){
                    
                   String rOrigen = comando[1];
                   String rDestino = comando[2];
                   String priCola = comando[3];
                   SendMessageRequest sendMessageRequest;
                   MessageJob mensaje;
                   if(priCola.equals("1")){
                      sendMessageRequest = new SendMessageRequest(colaAlta, rOrigen+";"+rDestino+";"+ip+";"+puerto);
                      sendMessageRequest.setMessageGroupId("mensajeAlta1");            
                      sendMessageRequest.setMessageDeduplicationId("1");
                      mensaje = MessageJob.newBuilder().setOrigin(rOrigen).setDestiny(rDestino).setPrior(MessageJob.Priority.HIGH).build();                      
                   }else{
                      sendMessageRequest = new SendMessageRequest(colaBaja, rOrigen+";"+rDestino+";"+ip+";"+puerto);
                      sendMessageRequest.setMessageGroupId("mensajeBaja1");            
                      sendMessageRequest.setMessageDeduplicationId("2");
                      mensaje = MessageJob.newBuilder().setOrigin(rOrigen).setDestiny(rDestino).setPrior(MessageJob.Priority.LOW).build();
                   }
                   
                   byte[] byteMensaje = mensaje.toByteArray();
                   
                   Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
                   messageAttributes.put("JobSend", new MessageAttributeValue().withDataType("Binary").withBinaryValue(ByteBuffer.wrap(byteMensaje)));            
                   sendMessageRequest.withMessageAttributes(messageAttributes);
                   
                   //Resultado del Mensaje
                   SendMessageResult sendMessageResult = sqs.sendMessage(sendMessageRequest);
                   String sequenceNumber = sendMessageResult.getSequenceNumber();
                   String messageId = sendMessageResult.getMessageId();
                   break;                   
                   
                   
               }else{
                    System.out.println("Comando Incorrecto");
               } 
            }
        
        }
        
    }
    
}
