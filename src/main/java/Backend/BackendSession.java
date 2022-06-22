package Backend;

import Backend.User;
//Author: @Smit_Thakkar
public final class BackendSession {

    private static User loggedInUser = null;

    private BackendSession() {

    }

    public static User getLoggedInUser() {
        return BackendSession.loggedInUser;
    }

    public static void setLoggedInUser(final User user) {
        BackendSession.loggedInUser = user;
    }
}