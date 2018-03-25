package pl.edu.rozpr.hashtable;


import org.jgroups.JChannel;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.protocols.pbcast.STATE;
import org.jgroups.protocols.pbcast.STATE_TRANSFER;
import org.jgroups.stack.ProtocolStack;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Scanner;

public class Main {

    private JChannel operationCh;
    private final String clusterName;
    private final ProtocolStack stack;
    public Main(String clusterName, InetAddress multicastAddress) {
        this.clusterName = clusterName;
        operationCh = new JChannel(false);
        stack = new ProtocolStack();
        stack.addProtocol(new UDP().setValue("mcast_group_addr", multicastAddress))
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
                .addProtocol(new FRAG2())
                .addProtocol(new STATE());
    }

    public void runInstance() {
        try {
            Scanner scanner = new Scanner(System.in);

            System.out.println("Starting on cluster " + this.clusterName);
            operationCh.setProtocolStack(stack);
            stack.init();
            operationCh.connect(this.clusterName);
            DistributedStringMap map = new DistributedStringMap(operationCh);

            System.out.println("Trying to get state...");
            operationCh.getState(null, 5000);

            String currentOption = "";
            do {
                if (!currentOption.isEmpty()) {

                    if (currentOption.equals("get")) {
                        System.out.println("Type key to obtain:");
                        String key = scanner.nextLine();
                        String value = map.get(key);
                        System.out.println("Key value: " + value);
                    }

                    if (currentOption.equals("contains")) {
                        System.out.println("Type key to check:");
                        String key = scanner.nextLine();
                        boolean result = map.containsKey(key);
                        System.out.println("Key exists: " + Boolean.toString(result));
                    }

                    if (currentOption.equals("put")) {
                        System.out.println("Type key and value in the next two following lines:");
                        String key = scanner.nextLine();
                        String value = scanner.nextLine();
                        map.put(key, value);
                        System.out.println("Done!");
                    }

                    if (currentOption.equals("remove")) {
                        System.out.println("Type key to remove:");
                        String key = scanner.nextLine();
                        map.remove(key);
                        System.out.println("Done!");
                    }

                }

                System.out.print("=> ");
                currentOption = scanner.nextLine();
            } while ( !currentOption.equals("q"));
            System.out.println("Detaching from the cluster...");
            operationCh.close();
        } catch (Exception e) {
            System.out.println("\nSomething went terribly wrong! Details:\n");
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        System.setProperty("java.net.preferIPv4Stack", "true");

        String clusterName = args.length == 1 ? args[0] : "default_group";
        try {
            Main application = new Main(clusterName, InetAddress.getByName("230.0.0.144"));
            application.runInstance();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }
}
