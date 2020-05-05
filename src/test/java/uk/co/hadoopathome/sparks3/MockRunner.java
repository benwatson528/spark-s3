package uk.co.hadoopathome.sparks3;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.adobe.testing.s3mock.util.HashUtil;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Objects;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
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
  private final S3Client s3Client = S3_MOCK.createS3ClientV2();
  @TempDir File outputPath;

  @DisplayName("Asserts that s3Mock is functioning")
  @Test
  void testBasicS3Functionality() throws IOException, NoSuchAlgorithmException {
    File inputFile = new File(INPUT_FILE_PATH);
    PutObjectRequest putObjectRequest =
        PutObjectRequest.builder().bucket(BUCKET_NAME).key(inputFile.getName()).build();
    this.s3Client.putObject(putObjectRequest, RequestBody.fromFile(inputFile));
    ResponseInputStream<GetObjectResponse> response =
        this.s3Client.getObject(
            GetObjectRequest.builder().bucket(BUCKET_NAME).key(inputFile.getName()).build());
    assertEquals(HashUtil.getDigest(new FileInputStream(inputFile)), HashUtil.getDigest(response));
  }

  @DisplayName(
      "Asserts that the main application runs when reading and writing to local (non-S3) locations")
  @Test
  void testRunSparkLocal() throws IOException {
    SparkSession sparkSession =
        SparkSession.builder().master("local").appName("Basic App").getOrCreate();
    File outputDir = new File(this.outputPath.toString() + "/output");
    SparkRunner.processSpark(
        new String[] {INPUT_FILE_PATH, outputDir.getAbsolutePath()}, sparkSession);
    File outputFile =
        Objects.requireNonNull(outputDir.listFiles((d, name) -> name.endsWith(".csv")))[0];
    List<String> outputContent = Files.readAllLines(outputFile.toPath());
    assertEquals(3, outputContent.size());
    assertEquals("name,subject", outputContent.get(0));
    assertEquals("ben,physics", outputContent.get(1));
    assertEquals("clo,marketing", outputContent.get(2));
  }
}
