package com.framedobjects

import java.io.FileOutputStream
import java.io.InputStream
import java.io.OutputStream

import scala.collection.JavaConverters.asScalaBufferConverter

import com.amazonaws.services.s3.model.ListObjectsRequest
import com.amazonaws.services.s3.model.S3ObjectSummary

import awscala.File
import awscala.Region
import awscala.s3.Bucket
import awscala.s3.S3
import awscala.s3.S3Object

object S3FileDownloader {

  implicit val region = Region.EU_WEST_1
  implicit val s3 = S3()

  def main(args: Array[String]) {
    val bucketName = "rtb-archive"
    val prefix = "mbr_notifications/20150109/opt_notif-error"

    val outputFolder = "/tmp/s3/"

    transferFilesFromS3(bucketName, prefix, outputFolder)
  }

  private def transferFilesFromS3(bucketName: String, prefix: String, outputFolder: String) {
    val bucket = s3.bucket(bucketName)
    println(s"Bucket: $bucket")
    bucket match {
      case Some(b) => {
        // Get list of object summaries
        val objectSummaries = s3.listObjects(new ListObjectsRequest().withBucketName(bucketName).withPrefix(prefix)).getObjectSummaries().asScala;
        println(s"found ${objectSummaries.size} objects");

        objectSummaries.foreach(fetchAndWriteFiles(_, b, outputFolder))
      }
      case None => println(s"No bucket found for name $bucketName")
    }
  }

  private def fetchAndWriteFiles(objectSummary: S3ObjectSummary, bucket: Bucket, outputFolder: String) {
    val key = objectSummary.getKey()
    println(s"fetching content for key $key having size of ${objectSummary.getSize()}")
    
    val fileName = key.split("/")(2)
    val fileObject = bucket.getObject(key)

    writeFileLocally(fileObject.get.getObjectContent(), new File(s"${outputFolder}/${fileName}"))
  }

  private def writeFileLocally(input: InputStream, to: File) {
    println(s"writing file ${to.getAbsoluteFile()}")
    val out = new FileOutputStream(to)
    try { transfer(input, out) }
    finally { out.close() }
  }

  /** Copies all bytes from the 'input' stream to the 'output' stream. */
  private def transfer(input: InputStream, out: OutputStream) {
    val buffer = new Array[Byte](8192)
    def transfer() {
      val read = input.read(buffer)
      if (read >= 0) {
        out.write(buffer, 0, read)
        transfer()
      }
    }
    transfer()
  }
}