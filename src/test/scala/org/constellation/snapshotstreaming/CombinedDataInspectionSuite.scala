package org.constellation.snapshotstreaming

import java.io.File
import java.nio.file.Files
import java.time.LocalDateTime
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import io.circe.parser.decode
import io.constellationnetwork.env.AppEnvironment
import io.constellationnetwork.schema.{GlobalIncrementalSnapshot, GlobalSnapshotInfo}
import pureconfig.ConfigSource
import pureconfig.generic.auto._
import pureconfig.module.enumeratum._
import eu.timepit.refined.pureconfig._
import weaver.SimpleIOSuite
import io.constellationnetwork.node.shared.config.types.SharedConfigReader
import io.constellationnetwork.node.shared.ext.pureconfig._
import io.constellationnetwork.security.signature.Signed

object CombinedDataInspectionSuite extends SimpleIOSuite {

  // Create the shared config from the configuration
  val sharedCfg = Configuration.nodeSharedConfig(AppEnvironment.Dev, ConfigSource.default.loadOrThrow[SharedConfigReader])

  test("Load and inspect combined data from testdata/combined") {
    for {
      // Load and deserialize the combined data
      _ <- IO.println("Reading testdata/combined file...")
      combinedJson <- IO.delay {
        val file = new File("testdata/combined")
        val content = new String(Files.readAllBytes(file.toPath))
        IO.println(s"File size: ${content.length} bytes").unsafeRunSync()
        content
      }
      
      _ <- IO.println("Parsing JSON data...")
        combinedData <- IO.fromEither(decode[(Signed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)](combinedJson))
      (snapshot, snapshotInfo) = combinedData
      
      _ <- IO.println("Successfully parsed the snapshot and snapshot info")
      _ <- IO.println(s"Snapshot ordinal: ${snapshot.value.ordinal}")
      _ <- IO.println(s"Snapshot height: ${snapshot.value.height}")
      
      // Basic validation
      _ <- IO.println("\nInspection complete - The combined data was successfully parsed")
      
    } yield success
  }
}
