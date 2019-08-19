import java.io.File

import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3ClientBuilder

object s3BucketConnector extends App {

  val s = " Season 8 "
  val f = s.trim.replaceAll(" ", "%20")
  println(f)
  println(f.contains(" "))



}
