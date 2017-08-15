package TestMain;

import TestMain.MessageImplementation.MessageJob;

import java.io.IOException;

import static TestMain.MessageImplementation.MessageJob.Priority.HIGH;

public class Main {


    public static void main(String [] args){

        System.out.println("HOLA");

        MessageJob message = MessageJob.newBuilder().setOrigin("~/test/file.txt")
                .setDestiny("~/comprimidos/test.zip").setPrior(HIGH).build();

        byte[] arregloemensaje = message.toByteArray();


        System.out.println(arregloemensaje);




    }

}
