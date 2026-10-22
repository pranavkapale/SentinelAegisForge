package io.sentinelaegisforge.streaming.kafka.featureavailability

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{FileAlreadyExistsException, Files, Path, StandardOpenOption}

/** Local single-writer durable claims; existing bytes are verified, never replaced. */
object FeatureReadyFiles {
  def claim(path: Path, bytes: Array[Byte]): Boolean = {
    Files.createDirectories(path.getParent)
    val pending = Files.createTempFile(path.getParent, ".pending-", ".json")
    try {
      val channel = FileChannel.open(pending, StandardOpenOption.WRITE)
      try {
        val buffer = ByteBuffer.wrap(bytes)
        while (buffer.hasRemaining) channel.write(buffer)
        channel.force(true)
      } finally channel.close()
      try {
        Files.createLink(path, pending)
        val directory = FileChannel.open(path.getParent, StandardOpenOption.READ)
        try directory.force(true)
        finally directory.close()
        true
      } catch {
        case _: FileAlreadyExistsException =>
          require(
            java.util.Arrays.equals(Files.readAllBytes(path), bytes),
            s"Conflicting artifact: $path"
          )
          false
      }
    } finally Files.deleteIfExists(pending)
  }

  def read(path: Path): Array[Byte] = {
    require(!Files.isSymbolicLink(path) && Files.isRegularFile(path), s"Invalid artifact: $path")
    Files.readAllBytes(path)
  }
}
