package mypipe.kafka.producer.stdout

import java.sql.Timestamp

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import mypipe.api.event._
import mypipe.api.data.ColumnType.ColumnValueString
import mypipe.api.producer.Producer
import org.slf4j.LoggerFactory
import com.typesafe.config.Config
import mypipe.api.data.Column
import mypipe.api.data.ColumnType._

class StdoutJsonProducer(config: Config) extends Producer(config) {

  protected val mutations = scala.collection.mutable.ListBuffer[String]()
  protected val log = LoggerFactory.getLogger(getClass)
  protected val objectMapper = new ObjectMapper()

  override def handleAlter(event: AlterEvent): Boolean = {
    log.info(s"\n$event\n")
    true
  }

  override def flush(): Boolean = {
    if (mutations.nonEmpty) {
      log.info("\n" + mutations.mkString("\n"))
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
        //case DATE | NEWDATE | TIMESTAMP | TIMESTAMP_V2 | DATETIME | DATETIME_V2 ⇒ output.put(entry._1, entry._2.value[Timestamp].toString)
        case _ ⇒ output.put(entry._1, entry._2.valueString)
      }
    })
    output
  }

  override def queue(mutation: Mutation): Boolean = {
    // TODO: quote column values if they are strings before printing
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
          mutations += payload.toString
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
          mutations += payload.toString
        })

      case d: DeleteMutation ⇒
        d.rows.foreach(row ⇒ {

          val payload = objectMapper.createObjectNode()
          payload.put("method", "DELETE")
          payload.put("table", s"${row.table.db}.${row.table.name}")
          payload.putNull("new")
          payload.set("old", colsToJson(row.columns))
          mutations += payload.toString
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

}
