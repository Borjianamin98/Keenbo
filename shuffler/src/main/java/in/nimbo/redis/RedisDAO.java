package in.nimbo.redis;

public interface RedisDAO {
    void add(String link);

    boolean contains(String link);
}
