# Creating student record using vectors
student_name <- c("Alice", "Bob", "Charlie", "David")
student_id <- c(101, 102, 103, 104)
student_age <- c(20, 21, 22, 23)
student_marks <- c(85, 90, 88, 95)

# Displaying the student record
print("Student Record:")
print(data.frame(Name = student_name, ID = student_id, Age = student_age, Marks = student_marks))

# Creating a data frame for medical patient status
patient_data <- data.frame(
  Patient_Age = c(45, 34, 28, 60),
  Gender = c("Male", "Female", "Female", "Male"),
  Symptoms = c("Fever, Cough", "Headache", "Chest Pain", "Nausea"),
  Patient_Status = c("Admitted", "Discharged", "Under Observation", "Critical")
)

# Displaying the patient data
print("Medical Patient Status:")
print(patient_data)



# Creating sample data for student marks
students <- c("Alice", "Bob", "Charlie", "David")
marks_math <- c(85, 92, 78, 90)
marks_science <- c(88, 85, 91, 83)

# Bar Chart
barplot(
  marks_math,
  names.arg = students,
  col = "skyblue",
  main = "Student Marks in Mathematics",
  xlab = "Students",
  ylab = "Marks"
)

# Scatter Plot
plot(
  marks_math,
  marks_science,
  main = "Scatter Plot of Marks in Math vs Science",
  xlab = "Math Marks",
  ylab = "Science Marks",
  col = "blue",
  pch = 16
)

# Adding labels to the scatter plot
text(
  marks_math,
  marks_science,
  labels = students,
  pos = 4,
  cex = 0.8,
  col = "red"
)
------------------------------------------------------------------------------------------------------------------------------------------------------------
Word count without combiner

mapper:
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split("\\s+"); // Split the line by whitespace

        for (String w : words) {
            word.set(w);
            context.write(word, one); // Emit the word with count 1
        }
    }
}

reducer:
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;

        // Sum up all the counts for each word
        for (IntWritable value : values) {
            sum += value.get();
        }

        context.write(key, new IntWritable(sum)); // Emit the word and its count
    }
}

driver:
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: WordCountDriver <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Word Count");

        job.setJarByClass(WordCountDriver.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
------------------------------------------------------------------------------------------------------------------------------------------------------------
Word count with combiner

mapper:
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split("\\s+");  // Split the line by spaces
        
        for (String str : words) {
            word.set(str);
            context.write(word, one);  // Emit each word with count 1
        }
    }
}

reducer:
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        context.write(key, new IntWritable(sum));  // Emit the final count of each word
    }
}

combiner:
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        context.write(key, new IntWritable(sum));  // Emit the sum for each word
    }
}

Driver:
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCountDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Word Count with Combiner");

        job.setJarByClass(WordCountDriver.class);
        job.setMapperClass(WordMapper.class);
        job.setCombinerClass(WordCombiner.class);  // Set Combiner
        job.setReducerClass(WordReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
------------------------------------------------------------------------------------------------------------------------------------------------------------
Matrix Multiplication
mapper:
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MatrixMapper extends Mapper<Object, Text, Text, Text> {

    private int matrixAColumns;  // Number of columns in matrix A
    private int matrixBColumns;  // Number of columns in matrix B

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        matrixAColumns = Integer.parseInt(context.getConfiguration().get("matrixAColumns"));
        matrixBColumns = Integer.parseInt(context.getConfiguration().get("matrixBColumns"));
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");
        String matrixName = tokens[0];
        int i = Integer.parseInt(tokens[1]);
        int j = Integer.parseInt(tokens[2]);
        int v = Integer.parseInt(tokens[3]);

        if (matrixName.equals("A")) {
            for (int k = 0; k < matrixBColumns; k++) {
                context.write(new Text(i + "," + k), new Text("A," + j + "," + v));
            }
        } else {
            for (int k = 0; k < matrixAColumns; k++) {
                context.write(new Text(k + "," + j), new Text("B," + i + "," + v));
            }
        }
    }
}

reducer:
import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MatrixReducer extends Reducer<Text, Text, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Maps to store the values of A and B corresponding to the key (i, j)
        HashMap<Integer, Integer> mapA = new HashMap<>();
        HashMap<Integer, Integer> mapB = new HashMap<>();

        for (Text val : values) {
            String[] tokens = val.toString().split(",");
            if (tokens[0].equals("A")) {
                mapA.put(Integer.parseInt(tokens[1]), Integer.parseInt(tokens[2]));
            } else {
                mapB.put(Integer.parseInt(tokens[1]), Integer.parseInt(tokens[2]));
            }
        }

        // Calculate the dot product
        int sum = 0;
        for (int k : mapA.keySet()) {
            if (mapB.containsKey(k)) {
                sum += mapA.get(k) * mapB.get(k);
            }
        }
        context.write(key, new IntWritable(sum));
    }
}

driver:
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MatrixMultiplicationDriver {

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: MatrixMultiplicationDriver <input path> <output path> <matrixAColumns> <matrixBColumns>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("matrixAColumns", args[2]);  // Set number of columns for matrix A
        conf.set("matrixBColumns", args[3]);  // Set number of columns for matrix B

        Job job = Job.getInstance(conf, "Matrix Multiplication");
        job.setJarByClass(MatrixMultiplicationDriver.class);
        job.setMapperClass(MatrixMapper.class);
        job.setReducerClass(MatrixReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
------------------------------------------------------------------------------------------------------------------------------------------------------------
Weather Data:
Driver
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WeatherDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: WeatherDriver <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Weather Data Analysis");

        job.setJarByClass(WeatherDriver.class);
        job.setMapperClass(WeatherMapper.class);
        job.setReducerClass(WeatherReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

Mappper
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WeatherMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text hotOrCold = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Split the input line into tokens
        String[] tokens = value.toString().split("\\s+");

        if (tokens.length >= 2) {
            // Extract the temperature (assuming it's the second token)
            float temperature = Float.parseFloat(tokens[1]);

            // Define hot or cold based on the temperature thresholds
            if (temperature > 30.0) {
                hotOrCold.set("Hot");
            } else if (temperature < 10.0) {
                hotOrCold.set("Cold");
            } else {
                return; // Skip moderate temperatures
            }

            context.write(hotOrCold, one); // Emit Hot or Cold with a count of 1
        }
    }
}

reducer
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WeatherReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;

        // Sum up the counts for hot or cold days
        for (IntWritable val : values) {
            sum += val.get();
        }

        context.write(key, new IntWritable(sum)); // Emit the result (Hot or Cold and the count)
    }
}

------------------------------------------------------------------------------------------------------------------------------------------------------------
Reading files
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
public class HdfsReader extends Configured implements Tool {
    
    public static final String FS_PARAM_NAME = "fs.defaultFS";
    
    public int run(String[] args) throws Exception {
        
        if (args.length < 2) {
            System.err.println("HdfsReader [hdfs input path] [local output path]");
            return 1;
        }
        
        Path inputPath = new Path(args[0]);
        String localOutputPath = args[1];
        Configuration conf = getConf();
        System.out.println("configured filesystem = " + conf.get(FS_PARAM_NAME));
        FileSystem fs = FileSystem.get(conf);
        InputStream is = fs.open(inputPath);
        OutputStream os = new BufferedOutputStream(new FileOutputStream(localOutputPath));
        IOUtils.copyBytes(is, os, conf);
        return 0;
    }
    
    public static void main( String[] args ) throws Exception {
        int returnCode = ToolRunner.run(new HdfsReader(), args);
        System.exit(returnCode);
    }
}
------------------------------------------------------------------------------------------------------------------------------------------------------------
Writing files
 import org.apache.hadoop.conf.Configured;
 import org.apache.hadoop.util.Tool;
 import java.io.BufferedInputStream;
 import java.io.FileInputStream;
 import java.io.InputStream;
 import java.io.OutputStream;
 import org.apache.hadoop.conf.Configuration;
 import org.apache.hadoop.fs.FileSystem;
 import org.apache.hadoop.fs.Path;
 import org.apache.hadoop.io.IOUtils;
 import org.apache.hadoop.util.ToolRunner;
 public class HdfsWriter extends Configured implements Tool {
    public static final String FS_PARAM_NAME = "fs.defaultFS";
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("HdfsWriter [local input path] [hdfs output path]");
            return 1;
        }
        String localInputPath = args[0];
        Path outputPath = new Path(args[1]);
        Configuration conf = getConf();
        System.out.println("configured filesystem = " + conf.get(FS_PARAM_NAME));
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            System.err.println("output path exists");
            return 1;
        }
        OutputStream os = fs.create(outputPath);
        InputStream is = new BufferedInputStream(new 
FileInputStream(localInputPath));
        IOUtils.copyBytes(is, os, conf);
        return 0;
    }
    public static void main( String[] args ) throws Exception {
        int returnCode = ToolRunner.run(new HdfsWriter(), args);
        System.exit(returnCode);
    }
 } 
------------------------------------------------------------------------------------------------------------------------------------------------------------

Spark:

spark-shell;
val data = sc.textFile("/Spark_Read/sparkwc.txt");
data.collect;
val splitteddata = data.flatMap(line => line.split(" ");
splitteddata.collect;
val mapdata = splitteddata.map(word =>(word,1));
mapdata.collect;
val reducedata = mapdata.reduceByKey(_+_);
reducedata.collect;

------------------------------------------------------------------------------------------------------------------------------------------------------------

Sqoop:

mysql -u root -p

#password <hadoop>

create database db;
use db;
create table employee(emp_id int primary key,emp_department varchar(50),salary int);

insert into employee values(10 ,'HR',10000),(20,'MR',20000);

exit;

sqoop import  
--connect jdbc:mysql://localhost/db
--username root
--password Hadoop
--table employee -m 1
--target-dir /dir 		#dir1 is the hdfs folder
--driver com.mysql.jdbc.Driver	

create table table2(emp_id int primary key,emp_department varchar(50),salary int);
exit;


sqoop export
--connect jdbc:mysql://localhost/db
--username root
--password Hadoop
--table table2 -m 1
--export-dir /dataemp/emp.txt  		# u should create a folder named 'dataemp' and have a txt file of the employee data as 'emp.txt'
--driver com.mysql.jdbc.Driver;