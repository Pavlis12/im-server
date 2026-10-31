package utb.fai;

import java.util.*;

public class ActiveHandlers {
    private static final long serialVersionUID = 1L;
    private HashSet<SocketHandler> activeHandlersSet = new HashSet<SocketHandler>();


    synchronized void sendMessageToAll(SocketHandler sender, String message) { 
        for (SocketHandler handler : activeHandlersSet) { 
            if (handler != sender) {
                if (!handler.messages.offer(message)) 
                    System.err.printf("Client %s message queue is full, dropping the message!\n", handler.clientID);
            }
        }
    }

    synchronized boolean add(SocketHandler handler) {
        return activeHandlersSet.add(handler);
    }

    
    synchronized boolean remove(SocketHandler handler) {
        return activeHandlersSet.remove(handler);
    }


    public synchronized void sendMessageToGroups(SocketHandler sender, String message) {
        for (SocketHandler client : activeHandlersSet) { 
            if (client != sender && client.clientName != null) {
                boolean shareGroup = !java.util.Collections.disjoint(sender.myGroups, client.myGroups);
                
                if (shareGroup) {
                    client.messages.offer(message);
                }
            }
        }
    }

    public synchronized void sendPrivate(String senderName, String targetName, String message) {
        String formattedMsg = "[" + senderName + "] >> " + message;
        
        for (SocketHandler client : activeHandlersSet) {
            if (client.clientName != null && client.clientName.equals(targetName)) {
                client.messages.offer(formattedMsg);
                break; 
            }
        }
    }
    public synchronized boolean isNameTaken(String name) {
    for (SocketHandler client : activeHandlersSet) {
        if (name.equals(client.clientName)) {
            return true;
        }
    }
    return false;
}
}