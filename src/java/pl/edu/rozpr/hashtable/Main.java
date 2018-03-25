package java.pl.edu.rozpr.hashtable;


import org.jgroups.JChannel;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.ProtocolStack;

public class Main {

    private JChannel operationCh;
    private final String clusterName;
    public Main(String clusterName) {
        this.clusterName = clusterName;
        operationCh = new JChannel(false);

        ProtocolStack stack = new ProtocolStack();
        stack.setProtocolStack(stack);
        stack.addProtocol(new UDP())
                .addProtocol(new PING())
                .addProtocol(new MERGE3())
                .addProtocol(new FD_SOCK())
                .addProtocol(new FD_ALL().setValue("timeout", 12000).setValue("interval", 3000))
                .addProtocol(new VERIFY_SUSPECT())
                .addProtocol(new BARRIER())
                .addProtocol(new NAKACK2())
                .addProtocol(new UNICAST3())
                .addProtocol(new STABLE())
                .addProtocol(new GMS())
                .addProtocol(new UFC())
                .addProtocol(new MFC())
                .addProtocol(new FRAG2());
    }

    public void runInstance() {
        try {
            operationCh.connect(this.clusterName);
        } catch (Exception e) {
            System.out.println("\nSomething wen terribly wrong! Details:\n");
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        String clusterName = args.length == 1 ? args[0] : "default_group";
        Main application = new Main(clusterName);
        application.runInstance();
    }
}
