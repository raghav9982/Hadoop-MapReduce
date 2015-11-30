import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class AverageMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable>
{

    public static final int MISSING = -9999;
    public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter)
            throws IOException
    {
        String line = value.toString();
        String [] path = line.split(",");
        String year = String.valueOf(Double.parseDouble(path[0].substring(0,4)));

       // double Month= Double.parseDouble(path[0].substring(5, 6));
        Double temperature;


        if(Month<= 6)
        {
            year=year.concat("Su");
        }
        else
            year=year.concat("Win");



        Double Tmaxtemp = Double.parseDouble(path[1].toString());
        Double Tmintemp = Double.parseDouble(path[2].toString());

        temperature = (Tmaxtemp+Tmintemp)/2;
        //String quality = line.substring(92, 93);
        // && quality.matches("[01459]")

        if(Tmaxtemp != MISSING && Tmintemp != MISSING) {
            output.collect(new Text(year), new DoubleWritable(temperature));
        }


        }

    }
