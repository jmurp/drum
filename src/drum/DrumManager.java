package drum;

import com.sleepycat.je.*;
import com.sleepycat.je.Durability.*;

import java.io.File;

public class DrumManager implements AutoCloseable {

    private final Environment dbEnv;
    private final Database drumDatabase;
    private final String drumName;
    private Transaction currentTxn;
    private final Durability durability;

    public DrumManager(final String envPath, final String drumName) {

        new File(envPath).mkdirs();

        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);

        dbEnv = new Environment(new File(envPath), envConfig);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);
        dbConfig.setBtreeComparator(new KeyComparator());
        this.drumDatabase = dbEnv.openDatabase(null, drumName, dbConfig);

        this.drumName = drumName;
        this.durability = new Durability(SyncPolicy.SYNC,SyncPolicy.SYNC,ReplicaAckPolicy.ALL);
        this.currentTxn = null;
    }

    private void beginTransaction() {
        currentTxn = dbEnv.beginTransaction(null,null);
    }

    private void commit() {
        currentTxn.commit(durability);
        currentTxn = null;
    }

    private void abort() {
        currentTxn.abort();
        currentTxn = null;
    }

    public void close() {
        drumDatabase.close();
        dbEnv.close();
    }

    public Cursor openCursor() {
        beginTransaction();
        return drumDatabase.openCursor(currentTxn,null);
    }

    public void closeCursor(Cursor cursor) {
        cursor.close();
        commit();
    }

    public static String findOnUpdate(String hash, Cursor cursor) throws Exception {
        DatabaseEntry key = new DatabaseEntry(hash.getBytes("UTF-8"));
        DatabaseEntry val = new DatabaseEntry();
        if (cursor.get(key,val,Get.SEARCH,null) != null) return new String(val.getData(),"UTF-8");
        return null;
    }

    public static boolean findOnCheck(String hash,Cursor cursor) throws Exception {
        DatabaseEntry key = new DatabaseEntry(hash.getBytes("UTF-8"));
        return cursor.get(key,null,Get.SEARCH,null) != null;
    }

    public static String getCurrentValue(String hash, Cursor cursor) throws Exception {
        DatabaseEntry key = new DatabaseEntry(hash.getBytes("UTF-8"));
        DatabaseEntry val = new DatabaseEntry();
        cursor.getCurrent(key,val,LockMode.RMW);
        return new String(val.getData(),"UTF-8");
    }
}
