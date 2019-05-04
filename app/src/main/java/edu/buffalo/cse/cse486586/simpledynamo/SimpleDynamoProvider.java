package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Base64;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

	String TAG="In content provider";
	private String ownPort;
	private int SERVER_PORT=10000;
	private boolean INITIALIZED;

	private class InnerComm{
		public Map<String, Integer> waiting_for = new HashMap<String, Integer>();
	}
	private InnerComm innerComm;

	private LinkedList<String> message_queue;
	private LinkedList<String> recovery_queue;
	private Nodes nodes;

	private final ExecutorService Pool_server = Executors.newSingleThreadExecutor();
	private final ExecutorService Pool_message = Executors.newSingleThreadExecutor();

	private SQLiteDatabase db;

	private String getOwnPort(){
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		return portStr;
	}

	private class Nodes {
		private String[] ports;
		private String[] serverPorts;
		private String[] hashes;
		private ArrayList<Integer> replicate;
		private ArrayList<Integer> ownReplicas;
		int ownIdx;

		public Nodes(String own_port){
			ports = new String[]{"5554", "5556", "5558", "5560", "5562"};
			serverPorts = new String[5];
			hashes = new String[5];
			replicate = new ArrayList<Integer>();

			//init server ports
			for (int i=0; i < 5; i++) {
				serverPorts[i] = String.valueOf(Integer.parseInt(ports[i]) * 2);
				hashes[i] = easyHash(ports[i]);
				if (own_port.equals(ports[i])) ownIdx = i;
			}

		}

		public String[] whosResponsible(String keyHash){
			int resp = 0;

			for (int i=0; i<5; i++){
				if (keyHash.compareTo(hashes[i]) < 0) {
					resp = i;
					break;
				}
			}
			return new String[]{serverPorts[resp],
								serverPorts[resp+1 % 4],
								serverPorts[resp+2 % 4]};
		}

		public String[] getReplicas(){
			return new String[]{serverPorts[ownIdx+1 % 4],
								serverPorts[ownIdx+2 % 4]};
		}

		public String[] getResplicating(){
			return new String[]{
					serverPorts[(ownIdx + 3) % 5],
					serverPorts[(ownIdx + 4) % 5]
			};
		}
	}

	private static class DatabaseHelper extends SQLiteOpenHelper {
		static final String DATABASE_NAME = "local_db";
		static final String MESSAGES_TABLE_NAME = "messages";
		static final int DATABASE_VERSION = 1;
		static final String TABLE_CONTENT =
				" (_id INTEGER PRIMARY KEY AUTOINCREMENT, " +
						" _key TEXT NOT NULL, " +
						" value TEXT NOT NULL);";
		static final String CREATE_TABLE_OWN = "CREATE TABLE own_data" + TABLE_CONTENT;
		static final String CREATE_TABLE_REPLICA1 = "CREATE TABLE replica1" + TABLE_CONTENT;
		static final String CREATE_TABLE_REPLICA2 = "CREATE TABLE replica2" + TABLE_CONTENT;

		DatabaseHelper(Context context){
			super(context, DATABASE_NAME, null, DATABASE_VERSION);
			context.deleteDatabase(DATABASE_NAME);
		}

		@Override
		public void onCreate(SQLiteDatabase db) {
			db.execSQL(CREATE_TABLE_OWN);
			db.execSQL(CREATE_TABLE_REPLICA1);
			db.execSQL(CREATE_TABLE_REPLICA2);
		}

		@Override
		public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
			db.execSQL("DROP TABLE IF EXISTS " +  MESSAGES_TABLE_NAME);
			onCreate(db);
		}
	}

	private class Record {
		private String key;
		private String value;
		private int[] vector_clock;
		private long timestamp;

		public Record(String encoding){
			Log.v(TAG, "parsing rec: "+ encoding);
			String[] split = encoding.split("\\.");
			try {
				key = new String(Base64.decode(split[0], Base64.DEFAULT), "UTF-8");
				value = new String(Base64.decode(split[1], Base64.DEFAULT), "UTF-8");
			} catch (UnsupportedEncodingException e) {Log.e(TAG, e.toString());}
			vector_clock = new int[5];
			for (int i=0; i<5; i++){
				vector_clock[i] = Integer.parseInt(split[i+2]);
			}
			timestamp = Long.parseLong(split[7]);
		}

		public Record(String key_, String val, long timest, int[] vclock){
			key = key_;
			value = val;
			timestamp = timest;
			vector_clock = vclock;
		}

		public String encode(){
			StringBuffer messageBody = new StringBuffer();
			try {
				messageBody.append(Base64.encodeToString(
							key.getBytes("UTF-8"),
							Base64.DEFAULT
					));
				messageBody.append('.');
				messageBody.append(Base64.encodeToString(
							value.getBytes("UTF-8"),
							Base64.DEFAULT
					));
				} catch (UnsupportedEncodingException e) {Log.e(TAG, e.toString());}

				for (int count: vector_clock) {
					messageBody.append('.');
					messageBody.append(count);
				}
				// key.value.c0.c1.c2.c3.c4
				messageBody.append('.');
				messageBody.append(timestamp);
				// keyB64.valueB64.c0.c1.c2.c3.c4.timestamp
				return messageBody.toString();
			}

		public void merge(Record newRecord){
			//TODO: implement merging according to vector clock as timestamp
		}
	}

	private Map<String, Map> valueStorage;

	private Map<String, Integer> sendConfirm;

	@Override
	public boolean onCreate() {
		//TODO: add recovery
		ownPort = getOwnPort();
		Log.v(TAG, "init ownPort: " + ownPort);

		nodes = new Nodes(ownPort);
		message_queue = new LinkedList<String>();
		recovery_queue = new LinkedList<String>();

		valueStorage = new HashMap<String, Map>();
		valueStorage.put("own", new HashMap<String, Record>());
		valueStorage.put("repl1", new HashMap<String, Record>());
		valueStorage.put("repl2", new HashMap<String, Record>());

		sendConfirm = new HashMap<String, Integer>();

		innerComm  = new InnerComm();

		Log.v(TAG, "initalizing database");

		Context context = getContext();
		DatabaseHelper dbHelper = new DatabaseHelper(context);
		db = dbHelper.getWritableDatabase();

		Log.v(TAG, "DB initialized");

		INITIALIZED = false;
		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(Pool_server, serverSocket);
			Log.v(TAG, "created serverSocket");
		} catch (Exception e){
			Log.e(TAG, e.toString());
		}
		new MessageProcessorTask().executeOnExecutor(Pool_message);
		new RecoveryManager();
		return db != null;
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		Log.v(TAG, "in insert");
		// TODO Auto-generated method stub
		String key = values.getAsString("key");
		String value = values.getAsString("value");
		long timestamp = System.currentTimeMillis();
		String keyHash = easyHash(key);
		String[] receivers = nodes.whosResponsible(keyHash);

		int[] vclock = {0,0,0,0,0};
		vclock[nodes.ownIdx] = 1; //marking own writing, receiver should just add these vectors
		Record toSend = new Record(key, value, timestamp, vclock);

		Log.v(TAG, "sendimg messages to: " + receivers[0]);
		new MessageSender("put.own." + toSend.encode(), receivers[0]).start();
		new MessageSender("put.repl1." + toSend.encode(), receivers[1]).start();
		new MessageSender("put.repl2." + toSend.encode(), receivers[2]).start();

		//message template put.keyB64.valueB64.c0.c1.c2.c3.c4.timestamp

		synchronized (sendConfirm){
			sendConfirm.put(key, 2);
			// waiting for W=2 nodes to respond
			while (sendConfirm.get(key) != 0){
				try {
					sendConfirm.wait();
				} catch (InterruptedException e) {
					Log.e(TAG, e.toString());
				}
			}
		}
		return null;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stubContentValues
		return null;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}

	private String easyHash(String input){
		try {
			return genHash(input);
		} catch (NoSuchAlgorithmException e){
			Log.e(TAG, "Hashing is not working");
		}
		return "something is wrong here";
	}

	private class ServerTask extends AsyncTask<ServerSocket, Void, Void> {
		private String TAG = "ServerTask";
		@Override
		protected Void doInBackground(ServerSocket... params) {
			ServerSocket serverSocket = params[0];
			Log.v(TAG, "server Started");
			Log.v(TAG, "Server listening to port: " + serverSocket.getLocalPort());

			String message = null;
			while (true) {
				try {
					Socket client_socket = serverSocket.accept();
					Scanner client_scanner = (new Scanner(client_socket.getInputStream(), "UTF8")).useDelimiter("\n");
					if (client_scanner.hasNext()) {
						message = client_scanner.next();
						client_socket.getOutputStream().write(7);
					}
					client_socket.close();
					Log.v(TAG, "Server received: " + message);

				} catch (SocketTimeoutException e){
					Log.e(TAG, "Server TimeOut Exception");
					break;
				} catch (IOException e) {
					Log.e(TAG, "server " + e.toString());
					break;
				}
				if (!INITIALIZED){
					if (message.substring(0,5).equals("recov")){
						synchronized (recovery_queue){
							recovery_queue.add(message);
							recovery_queue.notify();
						}
					} else {
						synchronized (message_queue) {
							message_queue.add(message);
							message_queue.notify();
						}
					}
				} else {
					synchronized (message_queue) {
						message_queue.add(message);
						message_queue.notify();
					}
				}
			}
			return null;
		}
	}

	private class MessageSender extends Thread {
		String message;
		String[] receivers;

		public MessageSender(String message, String remotePort) {
			this.message = message;
			this.receivers = new String[]{remotePort};
		}

		public MessageSender(String message, String[] remotePorts) {
			this.message = message;
			this.receivers = remotePorts;
		}

		private boolean sendMessage(String message, String remotePort) {
			message = message.replace("\n", "") + '\n';
			Log.v(TAG + " in send", "sending message: " + message + " to port " + remotePort);
			boolean failed = true;
			int n_failed = 0;

			while (failed){
				try {

					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(remotePort));

					socket.setSoLinger(true, 1000);
					socket.setSoTimeout(1000);

					OutputStreamWriter clientSocketWriter = (new OutputStreamWriter(socket.getOutputStream(), "UTF8"));


					clientSocketWriter.write(message, 0, message.length());
					clientSocketWriter.flush();

					int succ = socket.getInputStream().read();

					if (succ == 7) {
						Log.v(TAG, "Sent message to port: " + remotePort);
						socket.close();
						failed = false;
						break;
					} else {
						socket.close();
						throw new IOException("Din't receive confirmation");
					}
				}
				catch (UnknownHostException e) {
					Log.e(TAG, "in send: " + e.toString());
					break;
				} catch (SocketTimeoutException e){
					Log.e(TAG, "in send: " + e.toString());
					if (n_failed++ > 2) {
						break;
					}
				}catch (IOException e) {
					Log.e(TAG, "in send: " + e.toString());
					break;
				}
			}
			return failed;
		}

		public void run() {
			Log.v(TAG, "inMessageSender");
			for (String port: receivers){
				sendMessage(this.message, port);
			}
		}
	}

	private class MessageProcessorTask extends AsyncTask<LinkedList, Void, Void>{
		LinkedList<String> processed_queue;
		boolean finished=false;

//		private void processFind(String body){
//			String[] asking_keyHash = body.split("\\.",2);
//			if (myResponsibility(asking_keyHash[1])){
//				String response = "found." + selfPort + "." + prevPort;
//				sendMessage(response, asking_keyHash[0]);
//			} else {
//				sendMessage("find." + body, nextPort);
//			}
//		}
//
//		private void processFound(String body){
//			Log.v(TAG, "processingFound");
//			String[] selfAndPrev = body.split("\\.",2);
//			synchronized (innerComm.find){
//				innerComm.find.put("selfPort", selfAndPrev[0]);
//				innerComm.find.put("prevPort", selfAndPrev[1]);
//				innerComm.find.notify();
//			}
//			Log.v(TAG, "finishedFound");
//		}
//
//		private void processJoinedPrev(String body){
//			prevPort = body;
//			LinkedList<String> toRemove = new LinkedList<String>();
//			try {
//				prevHash = genHash(Integer.toString(Integer.parseInt(prevPort)/2));
//				synchronized (ownValues){
//					for (Map.Entry<String, String> entry : ownValues.entrySet()) {
//						String key = entry.getKey();
//						String value = entry.getValue();
//						if (genHash(key).compareTo(prevHash) <= 0){
//							sendMessage("put."+encodeKeyValue(key, value), prevPort);
//							toRemove.addLast(key);
//						}
//					}
//					for (String removeKey: toRemove){
//						ownValues.remove(removeKey);
//					}
//				}
//			} catch (NoSuchAlgorithmException e) {Log.e(TAG, e.toString());}
//		}
//
//		private void processJoinedNext(String body){
//			nextPort = body;
//		}
//
		private void processPut(String body){
			String[] owner_body = body.split("\\.",2);
			Record received = new Record(owner_body[1]);
			Map<String, Record> storage = valueStorage.get(owner_body[0]);

			synchronized (storage){ //if performance is bad - synchronize on record
				if (storage.containsKey(received.key)){
					storage.get(received.key).merge(received);
				} else {
					storage.put(received.key, received);
				}
			}
			Log.v(TAG, "inserted into "+owner_body[0]+" key;"+received.key+": "+received.value);
		}
//
//		private void processCount(String body){
//			Log.v(TAG, "received count");
//			String[] startCount = body.split("\\.",2);
//			if (startCount[0].equals(selfPort)) {
//				synchronized (innerComm.count){
//					innerComm.count.put("value", Integer.parseInt(startCount[1]));
//					innerComm.count.notify();
//				}
//			} else {
//				synchronized (ownValues){
//					int newCount = Integer.parseInt(startCount[1]) + ownValues.size();
//					sendMessage("count."+startCount[0]+"."+Integer.toString(newCount), nextPort);
//
//					for (Map.Entry<String, String> entry : ownValues.entrySet()) {
//						sendMessage("take." + encodeKeyValue(
//								entry.getKey(),entry.getValue()), startCount[0]);
//					}
//				}
//			}
//		}
//
//		private void processGet(String body){
//			String[] keyPort = decodeKeyValue(body);
//			String value = null;
//			synchronized (ownValues){
//				value = ownValues.get(keyPort[0]);
//			}
//			sendMessage("take."+encodeKeyValue(keyPort[0], value), keyPort[1]);
//		}
//
//		private void processTake(String body){
//			String[] keyVal = decodeKeyValue(body);
//			synchronized (tempValues){
//				tempValues.put(keyVal[0], keyVal[1]);
//				tempValues.notify();
//			}
//		}
//
//		private void processClear(String initPort){
//			if (!initPort.equals(selfPort)){
//				Log.v(TAG, "received a delete msg");
//				synchronized (ownValues){
//					ownValues.clear();
//					new MessageSender("sendMessage","clear."+initPort, nextPort).start();
//				}
//			}
//		}
//
//		private void processDelete(String body){
//			String[] keyPort = decodeKeyValue(body);
//			Log.v(TAG, "deleting " + keyPort[0]);
//			synchronized (ownValues){
//				ownValues.remove(keyPort[0]);
//			}
//		}
//
		private void processMessage(String message){
			String[] prefix_body = message.split("\\.",2);
			if (prefix_body[0].equals("put")) {
				processPut(prefix_body[1]);
			}
		}

		@Override
		protected Void doInBackground(LinkedList... params){
			processed_queue = params[0];
			while (!finished){
				synchronized (processed_queue){
					if (processed_queue.isEmpty()){
						try {
							processed_queue.wait();
						} catch (InterruptedException e) {
							Log.e(TAG, e.toString());
							break;
						}
					} else {
						processMessage(processed_queue.pop());
					}
				}
			}
			return null;
		}
	}

	private class RecoveryManager{
		public RecoveryManager() {
			//TODO: implement checking and recovery
			String[] repl12 = nodes.getReplicas();
			String[] _repl12 = nodes.getResplicating();
			String[] recepients = new String[]{repl12[0], repl12[1], _repl12[0], _repl12[1]};

			// getting own data
			new MessageSender("recov.fetch." + nodes.ownIdx, recepients);
		}

		private void processMessage(String message){
			String[] header_body = message.split("\\.");
			if (header_body[0].equals("recov_collect")){
				processCollect(header_body[1]);
			} else if (header_body[0].equals("other")){

			}
		}
		private void processCollect(String body){

		}
	}
} //ContentProvider