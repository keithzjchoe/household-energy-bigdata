import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class EnergyReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    private DoubleWritable result = new DoubleWritable();

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {

        double sum = 0.0;
        for (DoubleWritable val : values) {
            sum += val.get();
        }

        // Round to 2 decimal places
        result.set(Math.round(sum * 100.0) / 100.0);
        context.write(key, result);
    }
}
