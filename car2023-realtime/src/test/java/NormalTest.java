import com.atguigu.app.BaseApp;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

public class NormalTest extends BaseApp {

    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        stream.print();
    }

    public static void main(String[] args) {
        new NormalTest().start(10001, "Test", "ods_log");
    }
}
