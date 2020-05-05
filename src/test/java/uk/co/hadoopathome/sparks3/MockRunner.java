package uk.co.hadoopathome.sparks3;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.adobe.testing.s3mock.util.HashUtil;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class MockRunner {

  private static final String BUCKET_NAME = "test-bucket";

  @RegisterExtension
  static final S3MockExtension S3_MOCK =
      S3MockExtension.builder()
          .silent()
          .withSecureConnection(false)
          .withInitialBuckets(BUCKET_NAME)
          .build();

  private static final String INPUT_FILE_PATH = "src/test/resources/input_file.csv";
  private static final String OUTPUT_DIR = "src/test/resources/output/";
  private final S3Client s3Client = S3_MOCK.createS3ClientV2();

  @Test
  void basicS3Operations() throws IOException, NoSuchAlgorithmException {
    File inputFile = new File(INPUT_FILE_PATH);
    PutObjectRequest putObjectRequest =
        PutObjectRequest.builder().bucket(BUCKET_NAME).key(inputFile.getName()).build();
    this.s3Client.putObject(putObjectRequest, RequestBody.fromFile(inputFile));
    ResponseInputStream<GetObjectResponse> response =
        this.s3Client.getObject(
            GetObjectRequest.builder().bucket(BUCKET_NAME).key(inputFile.getName()).build());
    assertEquals(HashUtil.getDigest(new FileInputStream(inputFile)), HashUtil.getDigest(response));
  }

  @Test
  void runSparkLocalFiles() {
    SparkSession sparkSession = SparkSession.builder()
        .master("local")
        .appName("Basic App")
        .getOrCreate();
    SparkRunner.processSpark(new String[] {INPUT_FILE_PATH, OUTPUT_DIR}, sparkSession);
  }
}
