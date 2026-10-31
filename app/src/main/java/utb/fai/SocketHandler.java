package utb.fai;

import java.io.*;
import java.net.*;
import java.util.concurrent.*;
import java.util.Set;

public class SocketHandler {
	/** mySocket je socket, o který se bude tento SocketHandler starat */
	Socket mySocket;

	/** client ID je øetìzec ve formátu <IP_adresa>:<port> */
	String clientID; //předěláme na něco jiného idk co 
	String clientName;
	Set<String> myGroups = ConcurrentHashMap.newKeySet();
	ActiveHandlers activeHandlers;
	/**
	 * activeHandlers je reference na mnoinu vech právì bìících SocketHandlerù.
	 * Potøebujeme si ji udrovat, abychom mohli zprávu od tohoto klienta
	 * poslat vem ostatním!
	 */
	

	/**
	 * messages je fronta pøíchozích zpráv, kterou musí mít kaý klient svoji
	 * vlastní - pokud bude je pøetíená nebo nefunkèní klientova sí,
	 * èekají zprávy na doruèení právì ve frontì messages
	 */
	ArrayBlockingQueue<String> messages = new ArrayBlockingQueue<String>(20);

	/**
	 * startSignal je synchronizaèní závora, která zaøizuje, aby oba tasky
	 * OutputHandler.run() a InputHandler.run() zaèaly ve stejný okamik.
	 */
	CountDownLatch startSignal = new CountDownLatch(2);

	/** outputHandler.run() se bude starat o OutputStream mého socketu */
	OutputHandler outputHandler = new OutputHandler();
	/** inputHandler.run() se bude starat o InputStream mého socketu */
	InputHandler inputHandler = new InputHandler();
	/**
	 * protoe v outputHandleru nedovedu detekovat uzavøení socketu, pomùe mi
	 * inputFinished
	 */
	volatile boolean inputFinished = false;

	public SocketHandler(Socket mySocket, ActiveHandlers activeHandlers) {
		this.mySocket = mySocket;
		clientID = mySocket.getInetAddress().toString() + ":" + mySocket.getPort();
		this.activeHandlers = activeHandlers;
	}

	class OutputHandler implements Runnable {
		public void run() {
			OutputStreamWriter writer;
			try {
				System.err.println("DBG>Output handler starting for " + clientID);
				startSignal.countDown();
				startSignal.await();
				System.err.println("DBG>Output handler running for " + clientID);
				writer = new OutputStreamWriter(mySocket.getOutputStream(), "UTF-8");
				writer.write("\nYou are connected from " + clientID + "\n");
				writer.flush();
				while (!inputFinished) {
					String m = messages.take();// blokující ètení - pokud není ve frontì zpráv nic, uspi se!
					writer.write(m + "\r\n"); // pokud nìjaké zprávy od ostatních máme,
					writer.flush(); // poleme je naemu klientovi
					System.err.println("DBG>Message sent to " + clientID + ":" + m + "\n");
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.err.println("DBG>Output handler for " + clientID + " has finished.");

		}
	}

	class InputHandler implements Runnable {
		public void run() {
			try {
				System.err.println("DBG>Input handler starting for " + clientID);
				startSignal.countDown();
				startSignal.await();
				System.err.println("DBG>Input handler running for " + clientID);
				String request = "";
				/**
				 * v okamiku, kdy nás Thread pool spustí, pøidáme se do mnoiny
				 * vech aktivních handlerù, aby chodily zprávy od ostatních i nám
				 */
				
				BufferedReader reader = new BufferedReader(new InputStreamReader(mySocket.getInputStream(), "UTF-8"));
				while(clientName == null){
					messages.offer("Zadej jmeno bez mezer: ");
					
					String name = reader.readLine();

					if (name == null) {
       				 return; 
   					 } 
					 else if (name.trim().isEmpty() || name.contains(" "))
					 {
      				  messages.offer("Chyba: Jmeno nesmi byt prazdne ani obsahovat mezery!");
   					 } 
					 else 
					{
     				   clientName = name;
       				 myGroups.add("public");
    				}
					
				
				}
				activeHandlers.add(SocketHandler.this);


				while ((request = reader.readLine()) != null) {
   if (request.startsWith("#setMyName ")) {
    String newName = request.substring(11).trim();
    if (!newName.isEmpty() && !newName.contains(" ")) {
        if (!activeHandlers.isNameTaken(newName)) {
            clientName = newName;
        } else {
            messages.offer("Toto jmeno je jiz obsazene.");
        }
    } else {
        messages.offer("Jmeno nesmi byt prazdne ani obsahovat mezery.");
    }
}
    else if (request.startsWith("#join ")) {
        String roomName = request.substring(6).trim();
        if (!roomName.isEmpty()) {
            myGroups.add(roomName);
        }
    } 
    else if (request.startsWith("#leave ")) {
        String roomName = request.substring(7).trim();
        if (!roomName.isEmpty()) {
            myGroups.remove(roomName);
        }
    } 
    else if (request.equals("#groups")) {
        String groupsList = String.join(",", myGroups);
        messages.offer(groupsList);
    } 
    else if (request.startsWith("#sendPrivate ")) {
        String[] parts = request.split(" ", 3);
        if (parts.length == 3) {
            String targetName = parts[1];
            String privateMsg = parts[2];
           activeHandlers.sendPrivate(clientName, targetName, privateMsg);
        } else {
            messages.offer("Spatny format. Pouzij: #sendPrivate <jmeno> <zprava>");
        }
    } 
    else {
        String formattedMsg = "[" + clientName + "] >> " + request;
        System.out.println(formattedMsg);
        activeHandlers.sendMessageToGroups(SocketHandler.this, formattedMsg);
    }
}
				inputFinished = true;
				messages.offer("OutputHandler, wakeup and die!");
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} finally {
				synchronized (activeHandlers) {
					activeHandlers.remove(SocketHandler.this);
				}
			}
			System.err.println("DBG>Input handler for " + clientID + " has finished.");
		}

	}
}
