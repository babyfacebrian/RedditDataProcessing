import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JsonObjectRetriever {

    public static void main(String[] args) {

        String aws_access_key = new ProfileCredentialsProvider().getCredentials().getAWSAccessKeyId();
        String aws_secret_key = new ProfileCredentialsProvider().getCredentials().getAWSSecretKey();

        SparkSession sparkSession = SparkSession.builder().appName("JSON to DataFrame").master("local[*]").getOrCreate();
        sparkSession.sparkContext().hadoopConfiguration().set("fs.s3a.access.key", aws_access_key);
        sparkSession.sparkContext().hadoopConfiguration().set("fs.s3a.secret.key", aws_secret_key);

        String bucket = "s3a://reddit-data-sentiment-connect/sentiment-data-output/comments_processed/reddit_comments_NLP";
        Dataset<Row> dataset = sparkSession.read().format("json").load(bucket);

        dataset.show();
    }
}
