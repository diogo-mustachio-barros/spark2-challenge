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
}
