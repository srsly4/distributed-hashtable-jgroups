package pl.edu.rozpr.hashtable;

/**
 * Created by sirius on 25.03.18.
 */
public enum DistributedStringMapMessageType {
    KEY_PUT ((byte)0x01),
    KEY_REMOVE ((byte)0x02),
    ACQUIRE_STATE ((byte)0xa2);

    private final byte operationNumber;
    private DistributedStringMapMessageType(byte operationNumber) {
        this.operationNumber = operationNumber;
    }

    public byte getOperationNumber() { return this.operationNumber; }
}
