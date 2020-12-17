package com.box.dataplatform.iceberg

/**
 * Classes to hold iceberg partition information
 */
object Partition {
  val empty = Partition(Vector())
}

// Represents a partition as an ordered list of fields, i.e [field1=value1, field2=value2, ...]
case class Partition(fields: IndexedSeq[PartitionField]) {
  def addField(field: PartitionField): Partition = {
    // append (to end of vector)
    copy(fields = fields :+ field)
  }
}

case class PartitionField(fieldName: String, value: Any)
