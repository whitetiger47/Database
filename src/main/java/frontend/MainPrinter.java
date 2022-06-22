package frontend;
//Author: @Smit_Thakkar
public final class MainPrinter {

    private static MainPrinter instance;

    private MainPrinter() {
        // Required private constructor.
    }

    public static MainPrinter getInstance() {
        if (instance == null) {
            instance = new MainPrinter();
        }
        return instance;
    }

    public final void printScreenTitle(final String screenTitle) {
        System.out.println("**************************************************");
        System.out.println(screenTitle);
        System.out.println("**************************************************");
    }

    public final void printContent(final String content) {
        System.out.println(content);
    }



}