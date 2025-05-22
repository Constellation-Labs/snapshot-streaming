package org.constellation.snapshotstreaming

import java.io.File
import java.nio.file.Files
import java.time.LocalDateTime
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import io.circe.parser.decode
import io.constellationnetwork.env.AppEnvironment
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema.{GlobalIncrementalSnapshot, GlobalSnapshotInfo}
import pureconfig.ConfigSource
import pureconfig.generic.auto._
import pureconfig.module.enumeratum._
import eu.timepit.refined.pureconfig._
import weaver.SimpleIOSuite
import io.constellationnetwork.node.shared.config.types.SharedConfigReader
import io.constellationnetwork.node.shared.ext.pureconfig._
import io.constellationnetwork.security.signature.Signed
import org.constellation.snapshotstreaming.SnapshotProcessor
import org.constellation.snapshotstreaming.mapper.{CurrencySnapshotMapper, GlobalSnapshotMapper}
import io.constellationnetwork.security.{HashLogic, Hashed, Hasher}
import io.constellationnetwork.security.hash.{Hash, ProofsHash}
import cats.data.NonEmptyList

// wget http://52.53.46.33:9000/global-snapshots/latest/combined -O combined-$(date +%s)
object CombinedDataInspectionSuite extends SimpleIOSuite {

  // Create the shared config from the configuration
  val sharedCfg = Configuration.nodeSharedConfig(AppEnvironment.Dev, ConfigSource.default.loadOrThrow[SharedConfigReader])

  // Create a mock hasher for testing - only implement required methods
  implicit val hasher: Hasher[IO] = new Hasher[IO] {
    override def hash[A](data: A)(implicit encoder: io.circe.Encoder[A]): IO[Hash] = IO.pure(Hash.empty)
    override def compare[A](data: A, expectedHash: Hash)(implicit encoder: io.circe.Encoder[A]): IO[Boolean] = IO.pure(true)
    override def getLogic(ordinal: io.constellationnetwork.schema.SnapshotOrdinal): HashLogic = io.constellationnetwork.security.JsonHash
  }
  
  // Implicit JsonSerializer instance for IO
  implicit val jsonSerializer: JsonSerializer[IO] = JsonSerializer.forSync[IO].unsafeRunSync()
  
  // Create the mappers
  val globalMapper = GlobalSnapshotMapper.make[IO](sharedCfg)
  val currencyMapper = CurrencySnapshotMapper.make[IO]()

  test("Load, map and inspect GlobalData from combined snapshots") {
    for {
      // Load and deserialize the first combined file
      _ <- IO.println("Reading first combined file...")
      combinedJson <- IO.delay {
        val file = new File("testdata/combined-1747870502")
        val content = new String(Files.readAllBytes(file.toPath))
        IO.println(s"File size: ${content.length} bytes").unsafeRunSync()
        content
      }
      
      // Load and deserialize the second combined file
      _ <- IO.println("Reading second combined file...")
      combinedJson2 <- IO.delay {
        val file = new File("testdata/combined-1747870508")
        val content = new String(Files.readAllBytes(file.toPath))
        IO.println(s"File size: ${content.length} bytes").unsafeRunSync()
        content
      }
      
      // Parse the JSON data
      _ <- IO.println("Parsing JSON data...")
      combinedData <- IO.fromEither(decode[(Signed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)](combinedJson))
      (snapshot, snapshotInfo) = combinedData
      
      combinedData2 <- IO.fromEither(decode[(Signed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)](combinedJson2))
      (snapshot2, snapshotInfo2) = combinedData2
      
      // Print basic info about the snapshots
      _ <- IO.println("Successfully parsed the snapshot and snapshot info")
      _ <- IO.println(s"Snapshot ordinal: ${snapshot.value.ordinal}")
      _ <- IO.println(s"Snapshot ordinal2: ${snapshot2.value.ordinal}")
      
      // Create the timestamp for the snapshots
      timestamp = LocalDateTime.now()
      
      // Create a hashed snapshot for the GlobalSnapshotWithState
      hashedSnapshot2 = Hashed(snapshot2, Hash.empty, ProofsHash(Hash.empty.value))

      // Debug lastCurrencySnapshots in more detail
      _ <- IO.delay {
        snapshotInfo2.lastCurrencySnapshots.foreach { case (a, s) =>
          println(s"Address: $a")
          println(s"Snapshot: $s")
          println(s"Snapshot class: ${s.getClass.getName}")
          println(s"Snapshot fields: ${s.getClass.getDeclaredFields.map(_.getName).mkString(", ")}")
        }
      }
      
      // Create the currency snapshots map using the same approach as in the real processor
      _ <- IO.println("\n--- Creating currency snapshots map using proper function ---")
      
      // This is similar to what happens in the real processor's foldM function
      // We're creating a list of currency snapshots for each address
      currencySnapshots = snapshotInfo2.lastCurrencySnapshots.map { case (address, snapInfo) =>
        // Create a list of snapshots for each address
        // In the real processor, this might contain multiple snapshots
        // We're simulating that by creating a NonEmptyList with the snapshot
        
        // Handle the Either type - snapInfo can be either a Signed[CurrencySnapshot] or a tuple
        val hashedSnapshot = snapInfo match {
          case Left(signed) => Hashed(signed.value, Hash.empty, ProofsHash(Hash.empty.value))
          case Right((signed, _)) => Hashed(signed.value, Hash.empty, ProofsHash(Hash.empty.value))
        }
        
        // Create a NonEmptyList with the snapshot
        // This is similar to what the real processor would do when processing multiple snapshots
        (address, NonEmptyList.of(Left(hashedSnapshot)))
      }
      
      // Create the GlobalSnapshotWithState
      _ <- IO.println("\n--- Creating GlobalSnapshotWithState ---")
      globalSnapshotWithState = SnapshotProcessor.GlobalSnapshotWithState(
        hashedSnapshot2,
        Some(snapshotInfo), // Using first snapshot info as the "previous" one
        snapshotInfo2,
        currencySnapshots, // Using populated currency snapshots map
        timestamp
      )
      
      _ <- IO.println(s"Created GlobalSnapshotWithState with ${globalSnapshotWithState.currencySnapshots.size} currency snapshots")
      
      // Create the GlobalData
      _ <- IO.println("\n--- Creating GlobalData ---")
      globalData <- globalMapper.mapGlobalSnapshot(
        globalSnapshotWithState,
        timestamp,
        hasher,
        hasher
      )
      
      // Print information about the GlobalData
      _ <- IO.println("\n--- GlobalData Details ---")
      _ <- IO.println(s"Snapshot: ordinal=${globalData.snapshot.ordinal}, height=${globalData.snapshot.height}, hash=${globalData.snapshot.hash}")
      _ <- IO.println(s"Blocks: count=${globalData.blocks.size}")
      _ <- IO.println(s"Transactions: count=${globalData.txs.size}")
      _ <- IO.println(s"Balances: count=${globalData.balances.size}")
      _ <- IO.println(s"Proofs: count=${globalData.proofs.size}")
      _ <- IO.println(s"AllowSpends: count=${globalData.allowSpends.size}")
      _ <- IO.println(s"TokenLocks: count=${globalData.tokenLocks.size}")
      _ <- IO.println(s"TokenUnlocks: count=${globalData.tokenUnlocks.size}")
      _ <- IO.println(s"DelegatedStakingCreate: count=${globalData.delegatedStakingCreate.size}")
      _ <- IO.println(s"DelegatedStakingWithdraw: count=${globalData.delegatedStakingWithdraw.size}")
      _ <- IO.println(s"DelegatedStakingRewards: count=${globalData.delegatedStakingRewards.size}")
      _ <- IO.println(s"SpendTransactions: count=${globalData.spendTransactions.size}")
      _ <- IO.println(s"AllowSpendExpirations: count=${globalData.allowSpendExpirations.size}")
      
      // Print details about the snapshot
      _ <- IO.println("\n--- GlobalData Content Details ---")
      _ <- IO.println(s"Snapshot: ${globalData.snapshot}")
      
      // Print information about blocks if there are any
      _ <- if (globalData.blocks.nonEmpty) {
        for {
          _ <- IO.println(s"\nBlocks (showing up to 3):")
          _ <- IO.delay(globalData.blocks.take(3).foreach(block => println(s"  - ${block}")))
        } yield ()
      } else IO.unit
      
      // Print information about transactions if there are any
      _ <- if (globalData.txs.nonEmpty) {
        for {
          _ <- IO.println(s"\nTransactions (showing up to 3):")
          _ <- IO.delay(globalData.txs.take(3).foreach(tx => println(s"  - ${tx}")))
        } yield ()
      } else IO.unit
      
      // Print information about balances if there are any
      _ <- if (globalData.balances.nonEmpty) {
        for {
          _ <- IO.println(s"\nBalances (showing up to 3):")
          _ <- IO.delay(globalData.balances.take(3).foreach(balance => println(s"  - ${balance}")))
        } yield ()
      } else IO.unit
      
      _ <- IO.println("globalSnapshotWithState.currencySnapshots.size: " + globalSnapshotWithState.currencySnapshots.size)
      // Now we'll use the currencyMapper to iterate over all metagraphs in the global snapshot
      _ <- IO.println("\n=== Metagraph Data Inspection ===")
      _ <- IO.println("Iterating over metagraph snapshots in the global snapshot...")
      
      // Map the currency snapshots to get MetagraphData
      metagraphData <- currencyMapper.mapCurrencySnapshots(
        globalSnapshotWithState,
        timestamp,
        hasher,
        hasher
      )
      
      // Print completion message
      _ <- IO.println("\nInspection complete - Successfully created and printed GlobalData and MetagraphData")
    } yield success
  }
}
