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
import android.database.MatrixCursor;
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
	private boolean INITIALIZED = false;
	private int MIN_R = 2;
	private int MIN_W = 2;

	private class InnerComm{
		public Map<String, Record> waiting_for;
		public Map<String, Integer> recvConfirm;
		public Map<String, Integer> sendConfirm;

		public InnerComm(){
			waiting_for = new HashMap<String, Record>();
			recvConfirm = new HashMap<String, Integer>();
			sendConfirm = new HashMap<String, Integer>();
		}

		public void initRecv(String key){
			waiting_for.put(key, new Record(key, null));
			recvConfirm.put(key, MIN_R);
		}

		public void addRecv(String key, Record record){
			waiting_for.get(key).merge(record);
			int curr = recvConfirm.get(key);
			recvConfirm.put(key, curr - 1);
		}

		public boolean isReceived(String key){
			return recvConfirm.get(key) <= 0;
		}
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
		private int ownIdx;
		private String[] recovSend;
		private String[] recovSendAs;

		public Nodes(String own_port){
			ports = new String[]{"5554", "5556", "5558", "5560", "5562"};
			serverPorts = new String[5];
			hashes = new String[5];
			recovSend = new String[]{"own", "own.repl1", "", "repl1.repl2", "repl2"};
			recovSendAs = new String[]{"repl2", "repl1.repl2", "", "own.repl1", "own"};
			//init server ports
			for (int i=0; i < 5; i++) {
				serverPorts[i] = String.valueOf(Integer.parseInt(ports[i]) * 2);
				hashes[i] = easyHash(ports[i]);
				if (own_port.equals(ports[i])) ownIdx = i;
			}

		}
		private int whatToSend(int other){
			int diff = ownIdx - other;
			diff = (diff<-2) ? diff+5 : diff;
			diff = (diff>2) ? diff-5 : diff;
			return diff + 2;
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
								serverPorts[(resp+1) % 5],
								serverPorts[(resp+2) % 5]};
		}

		public String[] getOthers(){
			String[] recepients = new String[4];
			int offset=0;
			for (int i = 0; i < 5; i++) {
				if (i != nodes.ownIdx) {
					recepients[offset] = nodes.serverPorts[i];
					offset++;
					}
				}
			return recepients;
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
//			context.deleteDatabase(DATABASE_NAME);
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

		public boolean isEmpty(SQLiteDatabase db){
			Cursor c = db.rawQuery("SELECT * FROM own_data", null);
			Log.e("DATABASE", "database has: " + c.getCount() + " values");
			return c.getCount() == 0;
		}
	}

	private class Record {
		private String key;
		private String value;
		private int[] vector_clock;
		private long timestamp;

		public Record(String key_, String val){
			key = key_;
			value = val;
			vector_clock = new int[]{0,0,0,0,0};
			timestamp = 0;
		}

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
			if (this.olderThen(newRecord)) {
				value = newRecord.value;
				timestamp = newRecord.timestamp;
				vector_clock = newRecord.vector_clock;
				Log.v(TAG, "merge rewritten a record w key: " + key);
			}
		}

		public boolean olderThen(Record newRecord){
			if (timestamp < newRecord.timestamp) return true;

			boolean ge = true;
			for (int i=0; i < 5; i++){
				ge = ge & (vector_clock[i] >= newRecord.vector_clock[i]);
			}
			return !ge;
		}
	}

	private class Storage {
		private Map<String, Map> data;
		private Map<String, Integer> sync;

		Storage(){
			sync = new HashMap<String, Integer>();
			sync.put("own", 0);
			sync.put("repl1", 0);
			sync.put("repl2", 0);

			data = new HashMap<String, Map>();
			data.put("own", new HashMap<String, Record>());
			data.put("repl1", new HashMap<String, Record>());
			data.put("repl2", new HashMap<String, Record>());
		}

		public void putRecord(String partition, Record record){
			Map<String, Record> storage = data.get(partition);

			synchronized (storage){ //if performance is bad - synchronize on record
				if (storage.containsKey(record.key)){
					storage.get(record.key).merge(record);
				} else storage.put(record.key, record);
			}
			Log.v(TAG, "inserted into "+partition+" key;"+record.key+": "+record.value);
		}

		public Record getRecord(String partition, String key){
			return (Record) data.get(partition).get(key);
		}

		public boolean isSynced(){
			for (int i: sync.values())
				if (i < 2) return false;
			Log.v(TAG, "Storage synced");
			return true;
		}

		public void syncData(String partition, String recordStrings){
			Log.v(TAG, "syncing "+ partition);
			sync.put(partition, sync.get(partition)+1);
			Map<String, Record> storage = data.get(partition);

			synchronized (storage){
				for (String recString : recordStrings.split(";")){
					if (recordStrings.isEmpty()) break;
					Record newRec = new Record(recString);
					if (storage.containsKey(newRec.key)) {
						storage.get(newRec.key).merge(newRec);
					} else storage.put(newRec.key, newRec);
				}
			}
		}

		public String encodeStorage(String partition){
			Map<String, Record> storage = data.get(partition);
			StringBuffer res = new StringBuffer();
			String prefix = "";
			for (Record rec : storage.values()){
				res.append(prefix);
				res.append(rec.encode());
				prefix=";";
			}
			return res.toString();
		}
	}

	private Storage valueStorage;

//	private Map<String, Integer> innerComm.sendConfirm;

	@Override
	public boolean onCreate() {
		//TODO: add recovery
		ownPort = getOwnPort();
		Log.v(TAG, "init ownPort: " + ownPort);

		nodes = new Nodes(ownPort);
		message_queue = new LinkedList<String>();
		recovery_queue = new LinkedList<String>();

		valueStorage = new Storage();
		innerComm  = new InnerComm();

		Log.v(TAG, "initalizing database");

		Context context = getContext();
		DatabaseHelper dbHelper = new DatabaseHelper(context);
		db = dbHelper.getWritableDatabase();

		Log.v(TAG, "DB initialized");

		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(Pool_server, serverSocket);
			Log.v(TAG, "created serverSocket");
		} catch (Exception e){
			Log.e(TAG, e.toString());
		}
		//letting evetybody know you're alive

		if (dbHelper.isEmpty(db)){
			INITIALIZED = true;
			Log.e(TAG, "initializing database");
			//mark that instance is initiated
			ContentValues dummyVals = new ContentValues();
			dummyVals.put("_key", "dummyKey");
			dummyVals.put("value", "dummyVal");
			db.insert("own_data", null, dummyVals);
			db.close();

			new MessageProcessorTask().executeOnExecutor(Pool_message, message_queue);
		} else {
			Log.e(TAG, "initiating recovery");
			//initiate recovery
			new MessageProcessorTask().executeOnExecutor(Pool_message, recovery_queue);
			new MessageProcessorTask().executeOnExecutor(Pool_message, message_queue);

			new MessageSender("recov_fetch." + nodes.ownIdx, nodes.getOthers()).start();
		}
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

		Log.v(TAG, "sending messages to: " + receivers[0]);
		StringBuffer buf = new StringBuffer();
		String[] where  = new String[]{".own.", ".repl1.", ".repl2."};

		buf.append("put.");
		buf.append(nodes.serverPorts[nodes.ownIdx]);
		for (int i=0; i<3; i++){
			buf.append(where[i]);
			buf.append(toSend.encode());
			new MessageSender(buf.toString(), receivers[i]).start();
			buf.delete(9, buf.length());
		}
		// for some reason avd0 got 2 messages in this one
		// message template put.keyB64.valueB64.c0.c1.c2.c3.c4.timestamp

		synchronized (innerComm.sendConfirm){
			innerComm.sendConfirm.put(key, MIN_W);
			// waiting for W=2 nodes to respond
			while (innerComm.sendConfirm.get(key) > 0){
				try {
					innerComm.sendConfirm.wait();
				} catch (InterruptedException e) {
					Log.e(TAG, e.toString());
				}
			}
		}
		return null;
	}

	public void sendTo3(String prefix, String suffix, String[] recepients){
		String[] messages = new String[3];
		String[] partitions = new String[]{".own.",".repl1.",".repl2."};
		for (int i=0; i<3; i++){
			messages[i] = prefix + partitions[i] + suffix;
		}
		new MessageSender(messages, recepients).start();
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stubContentValues
		Log.e(TAG, "Querying " + selection);
		MatrixCursor result = new MatrixCursor(new String[] {"key","value"});

		// if looking for a specific key
		if (!(selection.equals("*") || selection.equals("@"))) {
			String[] owners = nodes.whosResponsible(easyHash(selection));

			Log.v(TAG, "locking innerComm.waiting_for");
			synchronized (innerComm.recvConfirm) {
				innerComm.initRecv(selection);

				sendTo3("read_one_request."+nodes.serverPorts[nodes.ownIdx], selection, owners);

				while (!innerComm.isReceived(selection))
					try {
						innerComm.recvConfirm.wait();
					} catch (InterruptedException e) {Log.e(TAG, e.toString());}
			}
			synchronized (innerComm.waiting_for){
				Record received = innerComm.waiting_for.get(selection);
				result.addRow(new Object[]{received.key, received.value });
			}
			//Logging
			result.moveToFirst();
			Log.e("OUTPUT", "returning: " + result.getString(0) + " " + result.getString(1));
			return result;
		}


		if (selection.equals("@"))
			for (String partition : new String[]{"own","repl1","repl2"})
				for (Record rec: (List<Record>) valueStorage.data.get(partition).values())
					result.addRow(new String[]{rec.key, rec.value});
		return result;
//		return null;
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
		String receiver;
		String[] messages;
		String[] receivers;

		public MessageSender(String message, String remotePort) {
			this.message = message;
			this.receiver = remotePort;
		}

		public MessageSender(String message, String[] remotePorts) {
			this.message = message;
			this.receivers = remotePorts;
		}

		public MessageSender(String[] messages, String[] receivers){
			this.messages = messages;
			this.receivers = receivers;
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
					if (n_failed++ > 3) break;
				} catch (SocketTimeoutException e){Log.v(TAG, "inMessageSender");
					Log.e(TAG, "in send: " + e.toString());
					if (n_failed++ > 3) break;
				}catch (IOException e) {
					Log.e(TAG, "in send: " + e.toString());
					if (n_failed++ > 3) break;
				}
			}
			return failed;
		}

		public void run(){
			if ((receiver != null) && (receiver != null))
				sendMessage(message, receiver);
			else{
				if ((message != null) && (receivers != null)) {
					for (String recv : receivers)
						sendMessage(message, recv);
				} else
					for (int i=0; i<receivers.length; i++)
						sendMessage(messages[i], receivers[i]);
				}
		}
	}

	enum Callbacks {
		put, put_confirm, recov_fetch, recov_sync, read_one_request, read_one_response,
		read_part_request, read_part_response
	}

	private class MessageProcessorTask extends AsyncTask<LinkedList, Void, Void>{
		LinkedList<String> processed_queue;
		boolean finished=false;

		private void processMessage(String message){
			String[] prefix_body = message.split("\\.",2);

			switch (Callbacks.valueOf(prefix_body[0])){
				case read_part_request:
					processReadPartRequest(prefix_body[1]);
					break;
				case read_part_response:
					processReadPartResponse(prefix_body[1]);
					break;
				case read_one_response:
					processReadOneResponse(prefix_body[1]);
					break;
				case read_one_request:
					processReadOneRequest(prefix_body[1]);
					break;
				case put:
					processPut(prefix_body[1]);
					break;
				case put_confirm:
					processPutConfirm(prefix_body[1]);
					break;
				case recov_fetch:
					processFetch(prefix_body[1]);
					break;
				case recov_sync:
					processSync(prefix_body[1]);
					break;
			}
		}

		private void processReadPartRequest(String body){

		}

		private void processReadPartResponse(String body){

		}

		private void processReadOneResponse(String body){
			synchronized (innerComm.recvConfirm){
				Record newRec = new Record(body);
				innerComm.addRecv(newRec.key, newRec);
				innerComm.recvConfirm.notify();
				Log.e(TAG, "received record " + newRec.key + " " + newRec.value);
			}
		}

		private void processReadOneRequest(String body){
			String[] sendTo_part_key = body.split("\\.",3);
			Record rec = valueStorage.getRecord(sendTo_part_key[1], sendTo_part_key[2]);
			new MessageSender("read_one_response." + rec.encode(), sendTo_part_key[0]).start();
		}

		private void processPutConfirm(String key){
			Log.v(TAG, "received confirmation");
			synchronized (innerComm.sendConfirm){
				int curr = innerComm.sendConfirm.get(key);
				innerComm.sendConfirm.put(key, curr-1);
				innerComm.sendConfirm.notify();
			}
		}

		private void processPut(String body){
			String[] from_owner_body = body.split("\\.",3);
			Record received = new Record(from_owner_body[2]);
			valueStorage.putRecord(from_owner_body[1], received);
			Log.v(TAG, "sending put confirmation");
			new MessageSender("put_confirm." + received.key, from_owner_body[0]).start();
		}

		private void processSync(String body){
			Log.v(TAG, "processing sync: " + body);
			String[] owner_body = body.split("\\.",2);
			valueStorage.syncData(owner_body[0], owner_body[1]);
			if (valueStorage.isSynced()) finished=true;
		}

		private void processFetch(String body){
			int who = Integer.parseInt(body);
			//determine which data this node needs
			int sendIdx = nodes.whatToSend(who);
			String[] send = nodes.recovSend[sendIdx].split("\\.");
			String[] sendAs = nodes.recovSendAs[sendIdx].split("\\.");

			String sentLog = "";
			for (int i=0; i < send.length; i++){
				if (!sendAs[i].isEmpty()) {
					new MessageSender("recov_sync." + sendAs[i] + "." + valueStorage.encodeStorage(send[i]), nodes.serverPorts[who]).start();
					sentLog += sendAs[i] + ",";
				}
			}
			Log.v(TAG, "processing fetch from " + body + "; Sent "+sendIdx +" " + sentLog);
		}

		@Override
		protected Void doInBackground(LinkedList... params){
			Log.v(TAG, "started MessageProcessor");
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
					} else processMessage(processed_queue.pop());
				}
			}
			return null;
		}
	}
} //ContentProvider