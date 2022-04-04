package diskmgr;

public class PageCounter {
    public static int readCounter;
    public static int writeCounter;

    public static void initialize() {
        readCounter = 0;
        writeCounter = 0;
    }

    public static void readIncrement() {
        readCounter++;
    }

    public static void writeIncrement() {
        writeCounter++;
    }
}