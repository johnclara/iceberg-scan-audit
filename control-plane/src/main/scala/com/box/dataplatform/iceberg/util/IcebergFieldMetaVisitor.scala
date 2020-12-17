package com.box.dataplatform.iceberg.util

import java.util

import com.google.common.base.Joiner
import org.apache.iceberg.Schema
import org.apache.iceberg.exceptions.ValidationException
import org.apache.iceberg.types.{Type, TypeUtil, Types}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class FieldMeta(fieldName: String, fieldId: Integer, fieldType: Type, fieldIsOptional: Boolean)

object IcebergFieldMetaVisitor {
  val DOT_SEPARATOR = '.'
  val DOT = Joiner.on(DOT_SEPARATOR)

  def getParents(elem: String) = {
    val s = elem.split(DOT_SEPARATOR)
    val field = s.last
    val parents = s.tail.scanLeft(s.head) {
      (x, y) =>
      x + DOT_SEPARATOR + y
    }.filter(_ != elem)
    (field, parents)
  }

  @tailrec
  def parentIsOptional(name: String, nameMap: Map[String, FieldMeta]): Boolean = {
    val possibleParent =
      if (name.contains(DOT_SEPARATOR)) {
        name.split(DOT_SEPARATOR).init.mkString(DOT_SEPARATOR.toString)
      } else ""

    if (nameMap.contains(possibleParent)) {
      nameMap(possibleParent).fieldIsOptional || parentIsOptional(possibleParent, nameMap)
    } else {
      false
    }
  }
}

class IcebergFieldMetaVisitor extends TypeUtil.SchemaVisitor[mutable.Map[String, FieldMeta]] {
  private val nameToMeta = new mutable.LinkedHashMap[String, FieldMeta]
  private val fieldNamesStack: util.Deque[String] = new util.LinkedList[String]()
  private val levelMap = new mutable.LinkedHashMap[String, mutable.ListBuffer[String]]
  override def beforeField(field: Types.NestedField): Unit = {
    fieldNamesStack.push(field.name)
  }

  override def afterField(field: Types.NestedField): Unit = {
    fieldNamesStack.pop()
  }

  override def beforeListElement(elementField: Types.NestedField): Unit = {
    // only add "element" to the name if the element is not a struct, so that names are more natural
    // for example, locations.latitude instead of locations.element.latitude
    if (!elementField.`type`.isStructType)
      beforeField(elementField)
  }

  override def afterListElement(elementField: Types.NestedField): Unit = {
    // only remove "element" if it was added
    if (!elementField.`type`.isStructType)
      afterField(elementField)
  }

  override def beforeMapKey(keyField: Types.NestedField): Unit = {
    beforeField(keyField)
  }

  override def afterMapKey(keyField: Types.NestedField): Unit = {
    afterField(keyField)
  }

  override def beforeMapValue(valueField: Types.NestedField): Unit = {
    // only add "value" to the name if the value is not a struct, so that names are more natural
    if (!valueField.`type`.isStructType)
      beforeField(valueField)
  }

  override def afterMapValue(valueField: Types.NestedField): Unit = {
    // only remove "value" if it was added
    if (!valueField.`type`.isStructType)
      afterField(valueField)
  }

  override def schema(schema: Schema, structResult: mutable.Map[String, FieldMeta]): mutable.Map[String, FieldMeta] = {
    nameToMeta
  }

  override def struct(struct: Types.StructType, fieldResults: java.util.List[mutable.Map[String, FieldMeta]]): mutable.Map[String, FieldMeta] = {
    nameToMeta
  }

  override def field(field: Types.NestedField, fieldResult: mutable.Map[String, FieldMeta]): mutable.Map[String, FieldMeta] = {
    addField(field.name, FieldMeta(field.name, field.fieldId, field.`type`, field.isOptional))
    nameToMeta
  }

  override def list(list: Types.ListType, elementResult: mutable.Map[String, FieldMeta]): mutable.Map[String, FieldMeta] = {
    list.fields.asScala.foreach { field =>
      // No need to visit as this is field named "element" of list
      //addField(field.name, FieldMeta(field.name, field.fieldId, field.`type`, field.isOptional))
    }
    nameToMeta
  }

  override def map(map: Types.MapType, keyResult: mutable.Map[String, FieldMeta], valueResult: mutable.Map[String, FieldMeta]): mutable.Map[String, FieldMeta] = {
    map.fields.asScala.foreach { field =>
      addField(field.name, FieldMeta(field.name, field.fieldId, field.`type`, field.isOptional))
    }
    nameToMeta
  }

  private def addField(name: String, fieldMeta: FieldMeta): Unit = {
    val fullName =
      if (!fieldNamesStack.isEmpty)
        IcebergFieldMetaVisitor.DOT.join(IcebergFieldMetaVisitor.DOT.join(fieldNamesStack.descendingIterator), name)
      else {
        name
      }

      val existingFieldMeta = nameToMeta.put(fullName, fieldMeta).getOrElse(null)
      if (existingFieldMeta != null && !("element" == name) && !("value" == name))
        throw new ValidationException("Invalid schema: multiple fields for name %s: %s and %s", fullName, existingFieldMeta.fieldId, fieldMeta.fieldId)

    val parentName =  if (!fieldNamesStack.isEmpty) IcebergFieldMetaVisitor.DOT.join(fieldNamesStack.descendingIterator) else "ROOT"
    levelMap.getOrElseUpdate(parentName, new ListBuffer[String]).append(fullName)
  }

  def getReverseLevelMap(): mutable.Map[String, ListBuffer[String]] = {
    mutable.LinkedHashMap(levelMap.toSeq.reverse: _*)
  }
}
