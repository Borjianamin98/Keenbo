package in.nimbo.dao.hbase;

import java.io.IOException;
import java.util.List;

public interface HBaseDAO extends AutoCloseable {
    @Override
    void close() throws IOException;

    boolean[] contains(List<String> links);
}
