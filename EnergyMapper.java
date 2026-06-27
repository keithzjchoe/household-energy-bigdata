import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class EnergyMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private Text date = new Text();
    private DoubleWritable power = new DoubleWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        // Skip header line
        if (line.startsWith("Date")) return;

        String[] fields = line.split(";");

        // Ensure minimum required fields exist
        if (fields.length < 3) return;

        String dateField = fields[0].trim();  // Column 0: Date (e.g., 16/12/2006)
        String powerField = fields[2].trim(); // Column 2: Global_active_power

        // Skip missing values marked as '?'
        if (powerField.equals("?") || dateField.isEmpty()) return;

        try {
            double activePower = Double.parseDouble(powerField);
            date.set(dateField);
            power.set(activePower);
            context.write(date, power);
        } catch (NumberFormatException e) {
            // Skip malformed rows silently
        }
    }
}
