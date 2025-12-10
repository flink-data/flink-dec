package class2;

import java.util.Set;

public class UserUrlSet {
    String user;
    Set<String> urls;

    public UserUrlSet(String user, Set<String> urls) {
        this.user = user;
        this.urls = urls;
    }

    public UserUrlSet() {
    }

    @Override
    public String toString() {
        return "UserUrlSet{" +
                "user='" + user + '\'' +
                ", urls=" + urls +
                '}';
    }
}
