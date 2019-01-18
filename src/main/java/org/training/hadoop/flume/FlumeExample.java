package org.training.hadoop.flume;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;

import java.nio.charset.Charset;

public class FlumeExample {

    public static void main(String[] args){
        MyRpcClientFacade clientFacade = new MyRpcClientFacade();
        clientFacade.init("192.168.222.128", 41414);
        //Send 10 event to the remote Flume agent.That agent should be configured to listen with AvroSource
        String msg = "Hello Flume!";
        for(int i=0;i<10;i++){
            clientFacade.sendDataToFlume(msg);
        }
        clientFacade.cleanUp();
    }

}

class MyRpcClientFacade{

    private RpcClient client;

    private String hostname;

    private int port;

    public void init(String hostname,int port){
        this.hostname = hostname;
        this.port = port;
        this.client = RpcClientFactory.getDefaultInstance(hostname,port);
        //this.client = RpcClientFactory.getThriftInstance(hostname,port);
    }

    public void sendDataToFlume(String data){
        //create a Flume Event Object that encapsulates the data
        Event event = EventBuilder.withBody(data, Charset.forName("UTF-8"));
        //send the event
        try {
            client.append(event);
        } catch (EventDeliveryException e) {
            client.close();
            client = null;
            client = RpcClientFactory.getDefaultInstance(hostname,port);
            // Thrift Client
            //client = RpcClientFactory.getThriftInstance(hostname,port);
            e.printStackTrace();
        }

    }

    public void cleanUp(){
        //Close the RPC connection
        client.close();
    }

}
