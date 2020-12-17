package com.box.dataplatform.iceberg.util

import org.apache.iceberg.types.Types._
import org.apache.iceberg.types.{TypeUtil, Types}
import org.apache.iceberg.{Schema, Table, UpdateSchema}
import org.slf4j.LoggerFactory

import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/*
 * Use SchemaVisitor to populate a map of fields for current and new schema
 * Subtract current schema fields from new Schema fields to derive new fields that need to be added
 *   iterate new fields in order (x, x.y, x.y.z), ignore fields with iceberg key names (key, value, element)
 *     if field is not optional, throw an exception
 *     if field have parents (x.y.z parents are x.y, and x) and parents are unprocessed
 *       add field to the nearest parent
 *     else add field to root
 *
 * Subtract new schema fields from current schema fields to derive deleted fields
 *   iterate to-be-deleted fields in reverse order (x.y.z, x.y, x), ignore fields with iceberg keynames
 *     if field is required, throw an exception
 *     else delete field
 *
 * Intersect current schema fields with new schema fields to derive common fields.
 *   iterate fields in reverse order (x.y.z, x.y, x)
 *     if type differs from current to new schema, and both are primitive types
 *       try type promote from old schema to new schema
 *         if type promotion fails
 *           if field is an optional field under current schema, delete existing field and replace
 *             with new schema field
 *           else throw exception
 *
 */

object IcebergSchemaUpdater {
  val log = LoggerFactory.getLogger(IcebergSchemaUpdater.getClass)
  val DoNotDeleteFields = Set("ds", "hour")

  private def fieldIsMissingOrUnequal(
      fieldName: String,
      meta1: FieldMeta,
      nameToMeta2: mutable.Map[String, FieldMeta]): Boolean =
    nameToMeta2.get(fieldName) match {
      case Some(meta2) =>
        if (meta1.fieldType.typeId() == meta2.fieldType.typeId()) {
          // .copy(fieldIsOptional = true)
          //&& meta1.fieldIsOptional == meta2.fieldIsOptional =>
          false
        } else {
          log.info(s"$fieldName has mismatch $meta1 vs $meta2 ")
          true
        }
      case _ =>
        log.info(s"$fieldName has mismatch $meta1 not found ")
        true
    }

  def schemaIsEqual(schema1: Schema, schema2: Schema): Boolean = {
    val visitor1 = new IcebergFieldMetaVisitor()
    val visitor2 = new IcebergFieldMetaVisitor()
    val nameToMeta1 = TypeUtil.visit(schema1, visitor1)
    val nameToMeta2 = TypeUtil.visit(schema2, visitor2)

    val fixSchemaOrderEnabled = sys.env.getOrElse("SCHEMA_REORDER_ENABLED", "false").toBoolean
    val keysEqual = if (fixSchemaOrderEnabled) {
      visitor1.getReverseLevelMap().equals(visitor2.getReverseLevelMap())
    } else {
      true
    }

    nameToMeta1.size == nameToMeta2.size && keysEqual &&
    !nameToMeta1.exists {
      case (fieldName, meta1) =>
        fieldIsMissingOrUnequal(fieldName, meta1, nameToMeta2)
    }
  }

  def fixOrder(
      fieldName: String,
      newSubFields: ListBuffer[String],
      currentSubFields: ListBuffer[String],
      updater: UpdateSchema): Unit = {
    val newIntersectOld = newSubFields.intersect(currentSubFields)
    if (newIntersectOld.size <= 0) {
      return
    }
    // ignore new fields
    val newFieldsIterator = newIntersectOld.iterator
    //capture added fields
    val addedFields = newSubFields.toList.diff(newIntersectOld.toList)
    // ignore deleted fields
    val currentFieldsIterator = currentSubFields.intersect(newSubFields).iterator

    // special case first element mismatch
    var isFirst = true
    var previousField: String = null
    while (newFieldsIterator.hasNext) {
      val newField = newFieldsIterator.next()
      if (!currentFieldsIterator.hasNext) return
      val oldField = currentFieldsIterator.next()
      if (!newField.equals(oldField)) {
        log.warn(s"order mismatch for field in $fieldName oldfield: $oldField newfield: $newField")
        if (isFirst) {
          log.warn(s"moving $newField to first position in $fieldName")
          updater.moveFirst(newField)
        } else {
          if (previousField != null) {
            log.warn(s"moving $newField after $previousField in $fieldName")
            updater.moveAfter(newField, previousField)
          } else {
            log.warn(s"not moving $newField as previous field is null")
          }
        }

      }
      previousField = newField
      isFirst = false
    }
    isFirst = true
    if (addedFields.size > 0 && newSubFields.size > addedFields.size) {
      val newFieldsIterator2 = newSubFields.iterator
      var newField = newFieldsIterator2.next()
      previousField = null
      addedFields.foreach { addedField =>
        while (addedField != newField) {
          previousField = newField
          newField = newFieldsIterator2.next()
          isFirst = false
        }
        try {
          if (isFirst) {
            log.warn(s"moving added field $addedField to first position in $fieldName")
            updater.moveFirst(addedField)
            isFirst = false
          } else if (previousField != null) {
            log.warn(s"moving added field $addedField after $previousField in $fieldName")
            updater.moveAfter(addedField, previousField)
          }
        } catch {
          case iae: java.lang.IllegalArgumentException =>
          // ignore as fieldId is missing
        }
      }
    }
  }

  def processFieldOrdering(
      updater: UpdateSchema,
      currentVisitor: IcebergFieldMetaVisitor,
      newVisitor: IcebergFieldMetaVisitor) = {
    val currentLevelMap = currentVisitor.getReverseLevelMap()
    val newLevelMap = newVisitor.getReverseLevelMap()

    newLevelMap.foreach {
      case (fieldName, newSubFields) =>
        currentLevelMap.get(fieldName).map { currentSubFields =>
          fixOrder(fieldName, newSubFields, currentSubFields, updater)
        }
    }

  }

  def updateSchema(currentSchema: Schema, newSchema: Schema, table: Table) = {
    val currentVisitor = new IcebergFieldMetaVisitor()
    val newVisitor = new IcebergFieldMetaVisitor()
    val currentMap = TypeUtil.visit(currentSchema, currentVisitor)
    val newMap = TypeUtil.visit(newSchema, newVisitor)
    table.refresh()
    var updater = table.updateSchema()

    def processAddedFields(): Unit = {

      val addedFields = newMap.filterNot {
        case (elem, elemValue) =>
          List("key", "value", "element").contains(elem.split(IcebergFieldMetaVisitor.DOT_SEPARATOR).last)
      } -- currentMap.keySet

      var processedAdds = List.empty[String]
      val addedFieldsSorted = ListMap(
        addedFields.toSeq.sortWith(
          _._1.split(IcebergFieldMetaVisitor.DOT_SEPARATOR).length < _._1
            .split(IcebergFieldMetaVisitor.DOT_SEPARATOR)
            .length): _*)
      addedFieldsSorted.foreach { addedField =>
        val fullFieldName = addedField._1
        val fieldType = addedField._2.fieldType
        val fieldIsOptional = addedField._2.fieldIsOptional

        val (field, parents) = IcebergFieldMetaVisitor.getParents(fullFieldName)

        if (parents.nonEmpty) {
          val nearestParent = parents.reduceLeft((x, y) => if (x.length > y.length) x else y)

          if (!processedAdds.toSet.contains(nearestParent) && !processedAdds.toSet.contains(fullFieldName)) {
            if (!fieldIsOptional) {
              throw new IllegalArgumentException(
                s"To remain compatible, cannot add new non-optional column: ${fullFieldName}.")
            }

            // Adding name to nearestParent
            updater.addColumn(nearestParent, field, fieldType)
          }
          // no-op, parent fields already processed
        } else {
          if (!fieldIsOptional) {
            throw new IllegalArgumentException(
              s"To remain compatible, cannot add new non-optional column: ${fullFieldName}.")
          }
          // no parent, add to root
          log.info(s"Adding new column $field with type $fieldType")
          updater.addColumn(field, fieldType)
        }
        processedAdds = processedAdds ++ List(fullFieldName)
      }
    }

    def processDeletedFields(): Unit = {
      // delete removed optional fields
      // throw exception is required fields are removed
      val deletedFields = currentMap.filterNot {
        case (elem, elemValue) =>
          List("key", "value", "element").contains(elem.split(IcebergFieldMetaVisitor.DOT_SEPARATOR).last)
      } -- newMap.keySet

      val deletedFieldsSorted = ListMap(
        deletedFields.toSeq.sortWith(
          _._1.split(IcebergFieldMetaVisitor.DOT_SEPARATOR).length > _._1
            .split(IcebergFieldMetaVisitor.DOT_SEPARATOR)
            .length): _*)

      deletedFieldsSorted.foreach { deletedField =>
        val fullFieldName = deletedField._1

        if (!DoNotDeleteFields.contains(fullFieldName)) {
          val fieldType = deletedField._2.fieldType
          val fieldIsOptional = deletedField._2.fieldIsOptional

          if (!fieldIsOptional) {
            throw new IllegalArgumentException(s"To remain compatible, cannot delete required field: ${fullFieldName}.")
          }
          log.warn(s"deleting field $fullFieldName")
          updater.deleteColumn(fullFieldName)
        }
      }
    }

    def processTypePromotion(): Unit = {
      // common fields
      // if there is mismatch between new and current schema fields types
      //   check type promotion
      //   if invalid type promotion
      //     if schema is optional, replace the field, else throw exception

      var processedPromotions = List.empty[String]
      val common = currentMap.keySet.intersect(newMap.keySet).map(k => (k, (currentMap(k), newMap(k)))).toMap
      val commonSorted = ListMap(
        common.toSeq.sortWith(
          _._1.split(IcebergFieldMetaVisitor.DOT_SEPARATOR).length > _._1
            .split(IcebergFieldMetaVisitor.DOT_SEPARATOR)
            .length): _*)

      commonSorted.foreach {
        case (common_key, (currentSchemaField, newSchemaField)) =>
          val currentFieldType = currentSchemaField.fieldType
          val newFieldType = newSchemaField.fieldType
          if (currentFieldType.isPrimitiveType && newFieldType.isPrimitiveType && currentFieldType != newFieldType) {
            // field under current schema is required
            // check type promotion: https://iceberg.incubator.apache.org/spec/#schema-evolution
            (currentFieldType, newFieldType) match {
              case (oldDecimalType: Types.DecimalType, newDecimalType: DecimalType) =>
                if (newDecimalType.precision() > oldDecimalType.precision() && newDecimalType.scale() == oldDecimalType
                      .scale()) {
                  updater.updateColumn(
                    common_key,
                    Types.DecimalType.of(newDecimalType.precision(), newDecimalType.scale()))
                }
              case (floatType: Types.FloatType, doubleType: Types.DoubleType) =>
                updater.updateColumn(common_key, Types.DoubleType.get())
              case (integerType: Types.IntegerType, longType: Types.LongType) =>
                updater.updateColumn(common_key, Types.LongType.get())
              case (someOldType, someNewType) =>
                // invalid type promotion
                log.info(s"invalid promotion type $someOldType to $someNewType for field $common_key")
                if (currentSchemaField.fieldIsOptional) {
                  /*
                  // field under current schema is optional, delete old field and replace with new field
                  updater.deleteColumn(common_key).commit()
                  updater = table.updateSchema()

                  // add new field with different type
                  val (field, parents) = IcebergFieldMetaVisitor.getParents(common_key)
                  if (parents.nonEmpty) {
                    if (!parents.toSet.subsetOf(processedPromotions.toSet)) {
                      val nearestParent = parents.reduceLeft((x, y) => if (x.length > y.length) x else y)
                      // Adding field to nearestParent
                      updater.addColumn(nearestParent, field, newFieldType)
                      // update processed list
                      processedPromotions = processedPromotions ++ parents ++ List(common_key)
                    }
                    // no-op, parent fields already processed
                  } else {
                    // no parent, add to root
                    updater.addColumn(field, newFieldType)
                  }*/
                } else {
                  throw new IllegalArgumentException(
                    s"Invalid type promotion for field ${common_key}, ${someOldType} to ${someNewType} is not supported.")
                }
            }
          }
      }
    }

    processDeletedFields()
    processAddedFields()
    val fixSchemaOrder = sys.env.getOrElse("SCHEMA_REORDER_ENABLED", "false").toBoolean
    if (fixSchemaOrder) {
      processFieldOrdering(updater, currentVisitor, newVisitor)
    }
    processTypePromotion()
    updater.commit()
  }
}
