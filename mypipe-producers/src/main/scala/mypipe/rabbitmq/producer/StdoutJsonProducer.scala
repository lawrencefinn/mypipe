package mypipe.rabbitmq.producer

import java.io.IOException
import java.net.URISyntaxException
import java.security.{KeyManagementException, NoSuchAlgorithmException}

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.rabbitmq.client._
import mypipe.api.event._
import mypipe.api.data.ColumnType.ColumnValueString
import mypipe.api.producer.Producer
import org.slf4j.LoggerFactory
import com.typesafe.config.Config
import mypipe.api.data.Column
import mypipe.api.data.ColumnType._

class StdoutJsonProducer(config: Config) extends Producer(config) {

  protected val mutations = scala.collection.mutable.ListBuffer[JsonNode]()
  protected val log = LoggerFactory.getLogger(getClass)
  protected val objectMapper = new ObjectMapper()
  protected var amqConnection: Option[Connection] = None
  protected var amqChan: Option[Channel] = None
  protected val EXCHANGE_NAME = "mypipe_push"

  override def handleAlter(event: AlterEvent): Boolean = {
    log.info(s"\n$event\n")
    true
  }

  override def flush(): Boolean = {
    if (mutations.nonEmpty) {
      log.info("\n" + mutations.map(line ⇒ line.toString).mkString("\n"))
      mutations.foreach(line ⇒ {
        send(line.toString, s"${line.get("method").asText()}.${line.get("table").asText()}", 3)
      })
      mutations.clear()
    }

    true
  }

  override def queueList(mutationz: List[Mutation]): Boolean = {
    mutationz.foreach(queue)
    true
  }

  private def colsToJson(columns: Map[String, Column]): JsonNode = {
    val output = objectMapper.createObjectNode()
    columns.foreach(entry ⇒ {
      entry._2.metadata.colType match {
        case TINY | SHORT | INT24 ⇒ output.put(entry._1, entry._2.value[Int])
        case LONG ⇒ output.put(entry._1, entry._2.value[Long])
        case DOUBLE | FLOAT | DECIMAL | NEWDECIMAL ⇒ output.put(entry._1, entry._2.value[Double])
        case NULL ⇒ output.putNull(entry._1)
        case VAR_STRING | BLOB | TINY_BLOB | MEDIUM_BLOB | LONG_BLOB ⇒ output.put(entry._1, new String(entry._2.value[Array[Byte]]))
        case _ ⇒ output.put(entry._1, entry._2.valueString)
      }
    })
    output
  }

  override def queue(mutation: Mutation): Boolean = {
    // TODO: quote column values if they are strings before printing
    val it = config.entrySet().iterator()
    while (it.hasNext) {
      val entry = it.next()
      log.info("Config key " + entry.getKey)
    }
    mutation match {

      case i: InsertMutation ⇒
        i.rows.foreach(row ⇒ {
          val payload = objectMapper.createObjectNode()
          payload.put("method", "INSERT")
          payload.put("table", s"${i.table.db}.${i.table.name}")
          payload.set("new", colsToJson(row.columns))
          payload.putNull("old")
          /*
          val names = i.table.columns.map(_.name)
          val values = i.table.columns.map { column ⇒
            try {
              row.columns(column.name).valueString
            } catch {
              case e: Exception ⇒
                log.error(s"Could not get column $column (returning empty string) due to ${e.getMessage}\n${e.getStackTraceString}")
                ""
            }
          }
          val query = s"INSERT INTO ${i.table.db}.${i.table.name} (${names.mkString(", ")}) VALUES (${values.mkString(", ")})"
          mutations += query
          */
          mutations += payload
        })

      case u: UpdateMutation ⇒
        u.rows.foreach(rr ⇒ {

          val old = rr._1
          val cur = rr._2
          val pKeyColNames = u.table.primaryKey.map(pKey ⇒ pKey.columns.map(_.name))

          val p = pKeyColNames.map(colName ⇒ {
            val cols = old.columns
            cols.filter(_._1.equals(colName))
            cols.head
          })
          val payload = objectMapper.createObjectNode()
          payload.put("method", "UPDATE")
          payload.put("table", s"${old.table.db}.${old.table.name}")
          payload.set("new", colsToJson(cur.columns))
          payload.set("old", colsToJson(old.columns))
          /*

          val pKeyVals = p.map(_._2.valueString)
          val where = pKeyColNames
            .map(_.zip(pKeyVals)
              .map(kv ⇒ kv._1 + "=" + kv._2))
            .map(_.mkString(", "))
            .map(w ⇒ s"WHERE ($w)").getOrElse("")

          val curValues = cur.columns.values.map(_.valueString)
          val colNames = u.table.columns.map(_.name)
          val updates = colNames.zip(curValues).map(kv ⇒ kv._1 + "=" + kv._2).mkString(", ")
          mutations += s"UPDATE ${u.table.db}.${u.table.name} SET ($updates) $where"
          */
          mutations += payload
        })

      case d: DeleteMutation ⇒
        d.rows.foreach(row ⇒ {

          val payload = objectMapper.createObjectNode()
          payload.put("method", "DELETE")
          payload.put("table", s"${row.table.db}.${row.table.name}")
          payload.putNull("new")
          payload.set("old", colsToJson(row.columns))
          mutations += payload
          /*

          val pKeyColNames = if (d.table.primaryKey.isDefined) d.table.primaryKey.get.columns.map(_.name) else List.empty[String]

          val p = pKeyColNames.map(colName ⇒ {
            val cols = row.columns
            cols.filter(_._1 == colName)
            cols.head
          })

          val pKeyVals = p.map(_._2.valueString)
          val where = pKeyColNames.zip(pKeyVals).map(kv ⇒ kv._1 + "=" + kv._2).mkString(", ")
          mutations += s"DELETE FROM ${d.table.db}.${d.table.name} WHERE ($where)"
          */

        })

      case _ ⇒ log.info(s"Ignored mutation: $mutation")

    }

    true
  }

  override def toString: String = {
    "StdoutProducer"
  }

  protected def initRabbitMQ(): Connection = {
    val factory = new ConnectionFactory()
    factory.setUri(config.getString("uri"))
    factory.setVirtualHost("/")
    amqConnection = Some(factory.newConnection())
    amqConnection.get
  }

  /** Declares the channel for publishing
   *
   *  @throws IOException
   */
  @throws[IOException]
  protected def declareChannel(): Channel = {

    amqChan = Some(amqConnection.getOrElse(initRabbitMQ()).createChannel())
    amqChan.get.exchangeDeclare(EXCHANGE_NAME, "topic", true)
    amqChan.get.confirmSelect
    amqChan.get
  }

  @throws[IOException]
  private def simpleSend(message: String, topic: String) {
    val messageBodyBytes: Array[Byte] = message.getBytes
    amqChan.getOrElse(declareChannel()).basicPublish(EXCHANGE_NAME, topic, MessageProperties.PERSISTENT_TEXT_PLAIN, messageBodyBytes)
  }

  /** Sends a persistent message with retries
   *
   *  @param message string representing message
   *  @param topic   topic to publish to on exchange
   *  @param retries number of times to retry in case of connection failure
   *  @throws IOException
   */
  @throws[IOException]
  @throws[NoSuchAlgorithmException]
  @throws[KeyManagementException]
  @throws[URISyntaxException]
  def send(message: String, topic: String, retries: Integer) {
    try {
      declareChannel()
      simpleSend(message, topic)
      amqChan.get.waitForConfirmsOrDie()
      amqChan.get.close()
    } catch {
      case ex: ShutdownSignalException ⇒ {
        initRabbitMQ()
        if (retries > 0) send(message, topic, retries - 1)
        else throw ex
      }
      case e: InterruptedException ⇒ {
        log.error("Exception using wait for confirms or die", e)
        amqChan.get.close()
        //this really should not happen since we are creating the channel with confirm select
      }
    }
    amqChan = None
  }

}
