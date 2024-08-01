
import fs2.io.file.Files
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder
import software.amazon.awssdk.services.bcmdataexports.model.Table

import java.io.File
import java.util.Base64
import scala.jdk.CollectionConverters.IterableHasAsScala

object DynamoZipStore {

//  private val client = AmazonDynamoDBClientBuilder.standard()
//    .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:4566", "us-west-2"))
//    .build()
//
//  private val dynamoDB = new DynamoDB(client)
//
//  def uploadDataToDynamoDB(tableName: String): Unit = {
//    println("Uploading data to DynamoDB")
//
//    val table: Table = dynamoDB.getTable(tableName)
//
//    val directoryPath = "path/to/your/local/files"
//    val folder = new File(directoryPath)
//
//    if (folder.exists() && folder.isDirectory) {
//            val files = folder.listFiles().filter(_.isFile)
//            files.foreach { file =>
//              val fileName = file.getName
//              val fileContent = Files[IO].readAll(file.toPath)
//              val fileContentBase64 = Base64.getEncoder.encodeToString(fileContent)
//
//              val outcome: PutItemOutcome = table.putItem(
//                new Item()
//                  .withPrimaryKey("FileID", fileName)
//                  .withString("Content", fileContentBase64)
//              )
//            }
//      println("Files uploaded successfully.")
//    } else {
//      println(s"Directory $directoryPath does not exist or is not a directory.")
//    }
//  }
//
//  def downloadDataFromDynamoDB(tableName: String): Unit = {
//    println("Downloading data from DynamoDB")
//
//    val table: Table = dynamoDB.getTable(tableName)
//
//    val scanSpec = new ScanSpec()
//    val items = table.scan(scanSpec)
//
//    val downloadPath = "path/to/save/files"
//
//    items.asScala.foreach { item =>
//      val fileId = item.getString("FileID")
//      val fileContentBase64 = item.getString("Content")
//      val fileContent = Base64.getDecoder.decode(fileContentBase64)
//
//      //      Files.write(Paths.get(s"$downloadPath/$fileId"), fileContent)
//    }
//
//    println("Files downloaded successfully.")
//  }

}
