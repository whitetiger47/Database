package Exception;
//Author: @Smit_Thakkar
public final class AuthException extends Exception {

    private final String errorMessage;

    public AuthException(final String errorMessage) {
        super(errorMessage);
        this.errorMessage = errorMessage;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    @Override
    public String toString() {
        return "AuthException{" +
                "errorMessage='" + errorMessage + '\'' +
                '}';
    }
}