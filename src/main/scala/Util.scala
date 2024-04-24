import java.io.File
object Util {

    // from: https://www.baeldung.com/scala/delete-directories-recursively
    def deleteRecursively(file: File) {
        if (file.isDirectory) {
            file.listFiles.foreach(deleteRecursively)
        }

        if (file.exists && !file.delete) {
            throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
        }
    }

    def parseSizeInMB(s: String): Option[Double] = {
        if (s.endsWith("G")) {
            safeParseDouble(s.dropRight(1)).map(_ * 1000) 
        } else if (s.endsWith("M")) {
            safeParseDouble(s.dropRight(1))
        } else if (s.endsWith("k")) {
            safeParseDouble(s.dropRight(1)).map(_ / 1000) 
        } else {
            None
        }
    }

    def parseDollarPrice(s: String): Option[Double] = {
        if (s.equals("0")) {
            Some(0.0)
        } else if (s.startsWith("$")) {
            Some(s.drop(1).toDouble) 
        } else {
            None
        }
    }

    def dollarsToEuros(dollars: Double): Double = {
        dollars * 0.9
    }

    def safeParseDouble(s: String): Option[Double] = {
        try { 
            val d = s.toDouble
            if (d.isNaN()) None else Some(d)
        } catch { 
            case _ : Throwable => None 
        }
    }
}
