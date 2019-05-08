package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import android.os.Bundle;
import android.app.Activity;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.TextView;

import java.util.concurrent.TimeUnit;

public class SimpleDynamoActivity extends Activity {
	private static String TAG = "in activity";

	private static Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	final static Uri provider_uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");

	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_simple_dynamo);
    
		TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

		findViewById(R.id.button).setOnClickListener(new View.OnClickListener() {
			@Override
			public void onClick(View v) {
				Log.v(TAG, "initiating TEST");
				for (int i=15; i < 25; i++){
					ContentValues cv = new ContentValues();
					cv.put("key", "key"+i);
					cv.put("value", "value"+i);
					getContentResolver().insert(provider_uri, cv);
					Log.v(TAG, "inserted pair " + i);
				}
				Log.v(TAG, "querying key19");
				Cursor res = getContentResolver().query(provider_uri, null, "key19", null, null);

				try {
					while (res.moveToNext()) {
						Log.i("OUTPUT", "key: " + res.getString(0) + " value: " + res.getString(1));
					}
				} finally {
					res.close();
				}

				// TESTING DELETE
				getContentResolver().delete(provider_uri, "key19", null);
				Log.i("OUTPUT", "deleted key19");

				Log.v(TAG, "querying *");
				Cursor res2 = getContentResolver().query(provider_uri, null, "*", null, null);

				try {
					while (res2.moveToNext()) {
						Log.i("OUTPUT", "key: " + res2.getString(0) + " value: " + res2.getString(1));
					}
				} finally {
					res2.close();
				}

				getContentResolver().delete(provider_uri, "@", null);

				Log.v(TAG, "querying @");
				Cursor res1 = getContentResolver().query(provider_uri, null, "@", null, null);

				try {
					while (res1.moveToNext()) {
						Log.i("OUTPUT", "key: " + res1.getString(0) + " value: " + res1.getString(1));
					}
				} finally {
					res1.close();
				}

				getContentResolver().delete(provider_uri, "*", null);

			}
		});
	}

	@Override
	public boolean onCreateOptionsMenu(Menu menu) {
		// Inflate the menu; this adds items to the action bar if it is present.
		getMenuInflater().inflate(R.menu.simple_dynamo, menu);
		return true;
	}
	
	public void onStop() {
        super.onStop();
	    Log.v("Test", "onStop()");
	}

}
