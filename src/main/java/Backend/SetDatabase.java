package Backend;

public class SetDatabase {
    private static SetDatabase instance = null;

    public static SetDatabase getInstance(){
        if(instance == null){
            instance = new SetDatabase();
        }
        return instance;
    }

    private SetDatabase(){

    }

    public String db = "";

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }
}
