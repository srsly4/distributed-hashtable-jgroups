package pl.edu.rozpr.hashtable;

import org.jgroups.Address;
import org.jgroups.Message;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;


public class DistributedStringMapMessage implements Serializable {


    private final DistributedStringMapMessageType messageType;
    private String key = "";
    private String value = "";

    private DistributedStringMapMessage(DistributedStringMapMessageType messageType) {
        this.messageType = messageType;
    }

    public byte[] toBytes() throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(byteArrayOutputStream);
        out.writeObject(this);
        out.flush();
        byte[] bytes = byteArrayOutputStream.toByteArray();
        byteArrayOutputStream.close();
        return bytes;
    }

    public static DistributedStringMapMessage createFromBytes(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = new ObjectInputStream(byteArrayInputStream);

        DistributedStringMapMessage obj = (DistributedStringMapMessage)ois.readObject();
        byteArrayInputStream.close();
        return obj;
    }

    public static DistributedStringMapMessage createPutMessage(String key, String value) {
        DistributedStringMapMessage message = new DistributedStringMapMessage(DistributedStringMapMessageType.KEY_PUT);
        message.key = key;
        message.value = value;
        return message;
    }

    public static DistributedStringMapMessage createRemoveMessage(String key) {
        DistributedStringMapMessage message = new DistributedStringMapMessage(DistributedStringMapMessageType.KEY_REMOVE);
        message.key = key;
        return message;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public DistributedStringMapMessageType getMessageType() {
        return messageType;
    }
}
