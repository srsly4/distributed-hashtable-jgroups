package pl.edu.rozpr.hashtable;


import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.Receiver;
import org.jgroups.View;

import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

public class DistributedStringMap implements SimpleStringMap {

    private final Map<String, String> innerMap = new HashMap<>();

    public DistributedStringMap(JChannel channel) {
        channel.setReceiver(new Receiver() {
            @Override
            public void viewAccepted(View new_view) {

            }

            @Override
            public void suspect(Address suspected_mbr) {

            }

            @Override
            public void block() {

            }

            @Override
            public void unblock() {

            }

            @Override
            public void receive(Message msg) {
                System.out.println("Received something!\n" + msg.getLength());
            }

            @Override
            public void getState(OutputStream output) throws Exception {
                System.out.println("Sent local state!");
                synchronized(innerMap) {
                    ObjectOutputStream oos = new ObjectOutputStream(output);
                    oos.writeObject(innerMap);
                }
            }

            @Override
            public void setState(InputStream input) throws Exception {
                System.out.println("Receiving local state!");
                synchronized(innerMap) {
                    innerMap.clear();
                    ObjectInputStream ios = new ObjectInputStream(input);
                    Map<String, String> tmpMap = (Map<String, String>)ios.readObject();
                    innerMap.putAll(tmpMap);
                }
            }

        });
    }

    @Override
    public boolean containsKey(String key) {
        return this.innerMap.containsKey(key);
    }

    @Override
    public String get(String key) {
        return this.innerMap.get(key);
    }

    @Override
    public String put(String key, String value) {
        this.innerMap.put(key, value);
        return value;
    }

    @Override
    public String remove(String key) {
        this.innerMap.remove(key);
        return null;
    }
}
