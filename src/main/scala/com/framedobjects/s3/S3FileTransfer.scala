package com.framedobjects.s3

import awscala.Region
import awscala.s3.S3
import java.io.File
import java.io.FileOutputStream
import java.io.OutputStream
import java.io.InputStream

object S3FileTransfer {

  implicit val region = Region.EU_WEST_1
  implicit val s3 = S3()

  def transferFile(bucketName: String, objectKey: String, outputFile: File) {
    val bucket = s3.bucket(bucketName)
    println(s"Bucket: $bucket")
    bucket match {
      case Some(b) => {
        val fileObject = b.getObject(objectKey)
        fileObject match {
          case Some(o) => writeFileLocally(o.getObjectContent(), outputFile)
          case None => println(s"ERROR: no object for key $objectKey")
        }
      }
      case None => println(s"ERROR: No bucket found for name $bucketName")
    }
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