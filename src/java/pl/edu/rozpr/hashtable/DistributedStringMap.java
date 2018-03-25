package pl.edu.rozpr.hashtable;


import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.Receiver;
import org.jgroups.View;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class DistributedStringMap implements SimpleStringMap {

    private final Map<String, String> innerMap = new HashMap<>();
    private final JChannel channel;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition putCondition = lock.newCondition();
    private final Condition removeCondition = lock.newCondition();

    public DistributedStringMap(JChannel channel) {
        this.channel = channel;
        this.channel.setReceiver(new Receiver() {
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
                try {
                    DistributedStringMapMessage distMsg = DistributedStringMapMessage.createFromBytes(msg.getBuffer());
                    if (distMsg.getMessageType().equals(DistributedStringMapMessageType.KEY_PUT)) {
                        System.out.println(String.format("Received put message for '%s:%s'",
                                distMsg.getKey(), distMsg.getValue()));
                        lock.lock();
                        innerMap.put(distMsg.getKey(), distMsg.getValue());
                        putCondition.signalAll();
                        lock.unlock();
                    }
                    else if (distMsg.getMessageType().equals(DistributedStringMapMessageType.KEY_REMOVE)) {
                        System.out.println(String.format("Received remove message for key '%s'", distMsg.getKey()));
                        lock.lock();
                        innerMap.remove(distMsg.getKey());
                        removeCondition.signalAll();
                        lock.unlock();
                    } else {
                        System.out.println("Unknown message type!");
                    }
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
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
        try {
            lock.lock();
            DistributedStringMapMessage msg = DistributedStringMapMessage.createPutMessage(key, value);
            this.channel.send(null, msg.toBytes());

            while (!this.innerMap.containsKey(key) || !this.innerMap.get(key).equals(value))
                putCondition.await();

            lock.unlock();
            return value;
        }
        catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public String remove(String key) {
        try {
            lock.lock();
            DistributedStringMapMessage msg = DistributedStringMapMessage.createRemoveMessage(key);
            this.channel.send(null, msg.toBytes());

            while (this.innerMap.containsKey(key))
                removeCondition.await();

            lock.unlock();
            this.innerMap.remove(key);
            return key;
        }
        catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
