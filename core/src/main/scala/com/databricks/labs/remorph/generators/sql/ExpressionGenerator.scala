package com.databricks.labs.remorph.generators.sql

<<<<<<< HEAD
import com.databricks.labs.remorph.generators.GeneratorContext
<<<<<<< HEAD
import com.databricks.labs.remorph.parsers.intermediate.Expression
=======
import com.databricks.labs.remorph.parsers.intermediate.Literal
>>>>>>> af3e153a (Generate date and timestamp literals as separate methods (#722))
=======
import com.databricks.labs.remorph.generators.{Generator, GeneratorContext}
<<<<<<< HEAD
>>>>>>> 6328f493 (Feature: introduce core transpiler (#715))
=======
import com.databricks.labs.remorph.parsers.intermediate.RLike
>>>>>>> f85e0f10 (Snowflake: introduce a specific call mapper (#742))
import com.databricks.labs.remorph.parsers.{intermediate => ir}
import com.databricks.labs.remorph.transpilers.TranspileException

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate, LocalDateTime, ZoneId, ZonedDateTime}
import java.util.Locale

class ExpressionGenerator(val callMapper: ir.CallMapper = new ir.CallMapper())
    extends Generator[ir.Expression, String] {
  private val dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val timeFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneId.of("UTC"))

  override def generate(ctx: GeneratorContext, tree: ir.Expression): String = expression(ctx, tree)

  def expression(ctx: GeneratorContext, expr: ir.Expression): String = {
    expr match {
      case l: ir.Like => like(ctx, l)
      case r: ir.RLike => rlike(ctx, r)
      case _: ir.Bitwise => bitwise(ctx, expr)
      case _: ir.Arithmetic => arithmetic(ctx, expr)
      case _: ir.Predicate => predicate(ctx, expr)
      case l: ir.Literal => literal(ctx, l)
      case fn: ir.Fn => callFunction(ctx, fn)
      case ir.UnresolvedAttribute(name, _, _) => name
      case d: ir.Dot => dot(ctx, d)
      case i: ir.Id => id(ctx, i)
      case o: ir.ObjectReference => objectReference(ctx, o)
      case a: ir.Alias => alias(ctx, a)
      case d: ir.Distinct => distinct(ctx, d)
      case s: ir.Star => star(ctx, s)
      case c: ir.Column => column(ctx, c)
      case x => throw TranspileException(s"Unsupported expression: $x")
    }
  }

  private def arithmetic(ctx: GeneratorContext, expr: ir.Expression): String = expr match {
    case ir.UMinus(child) => s"-${expression(ctx, child)}"
    case ir.UPlus(child) => s"+${expression(ctx, child)}"
    case ir.Multiply(left, right) => s"${expression(ctx, left)} * ${expression(ctx, right)}"
    case ir.Divide(left, right) => s"${expression(ctx, left)} / ${expression(ctx, right)}"
    case ir.Mod(left, right) => s"${expression(ctx, left)} % ${expression(ctx, right)}"
    case ir.Add(left, right) => s"${expression(ctx, left)} + ${expression(ctx, right)}"
    case ir.Subtract(left, right) => s"${expression(ctx, left)} - ${expression(ctx, right)}"
  }

  private def bitwise(ctx: GeneratorContext, expr: ir.Expression): String = expr match {
    case ir.BitwiseOr(left, right) => s"${expression(ctx, left)} | ${expression(ctx, right)}"
    case ir.BitwiseAnd(left, right) => s"${expression(ctx, left)} & ${expression(ctx, right)}"
    case ir.BitwiseXor(left, right) => s"${expression(ctx, left)} ^ ${expression(ctx, right)}"
    case ir.BitwiseNot(child) => s"~${expression(ctx, child)}"
  }

  private def like(ctx: GeneratorContext, like: ir.Like): String = {
    val escape = if (like.escapeChar != '\\') s" ESCAPE '${like.escapeChar}'" else ""
    s"${expression(ctx, like.left)} LIKE ${expression(ctx, like.right)}$escape"
  }

  private def rlike(ctx: GeneratorContext, r: RLike): String = {
    s"${expression(ctx, r.left)} RLIKE ${expression(ctx, r.right)}"
  }

  private def predicate(ctx: GeneratorContext, expr: ir.Expression): String = expr match {
    case ir.And(left, right) => s"(${expression(ctx, left)} AND ${expression(ctx, right)})"
    case ir.Or(left, right) => s"(${expression(ctx, left)} OR ${expression(ctx, right)})"
    case ir.Not(child) => s"NOT (${expression(ctx, child)})"
    case ir.Equals(left, right) => s"${expression(ctx, left)} = ${expression(ctx, right)}"
    case ir.NotEquals(left, right) => s"${expression(ctx, left)} != ${expression(ctx, right)}"
    case ir.LessThan(left, right) => s"${expression(ctx, left)} < ${expression(ctx, right)}"
    case ir.LessThanOrEqual(left, right) => s"${expression(ctx, left)} <= ${expression(ctx, right)}"
    case ir.GreaterThan(left, right) => s"${expression(ctx, left)} > ${expression(ctx, right)}"
    case ir.GreaterThanOrEqual(left, right) => s"${expression(ctx, left)} >= ${expression(ctx, right)}"
    case _ => throw new IllegalArgumentException(s"Unsupported expression: $expr")
  }

  private def callFunction(ctx: GeneratorContext, fn: ir.Fn): String = {
    val call = callMapper.convert(fn)
    call match {
      case r: RLike => rlike(ctx, r)
      case fn: ir.Fn => s"${fn.prettyName}(${fn.children.map(expression(ctx, _)).mkString(", ")})"
      case _ => throw TranspileException("not implemented")
    }

  }

  private def literal(ctx: GeneratorContext, l: ir.Literal): String = {
    l.dataType match {
      case ir.NullType => "NULL"
      case ir.BinaryType => orNull(l.binary.map(_.map("%02X" format _).mkString))
      case ir.BooleanType => orNull(l.boolean.map(_.toString.toUpperCase(Locale.getDefault)))
      case ir.ShortType => orNull(l.short.map(_.toString))
      case ir.IntegerType => orNull(l.integer.map(_.toString))
      case ir.LongType => orNull(l.long.map(_.toString))
      case ir.FloatType => orNull(l.float.map(_.toString))
      case ir.DoubleType => orNull(l.double.map(_.toString))
      case ir.StringType => orNull(l.string.map(singleQuote))
      case ir.DateType => dateLiteral(l)
      case ir.TimestampType => timestampLiteral(l)
      case ir.ArrayType(_) => orNull(l.array.map(arrayExpr(ctx)))
      case ir.MapType(_, _) => orNull(l.map.map(mapExpr(ctx)))
      case _ => throw new IllegalArgumentException(s"Unsupported expression: ${l.dataType}")
    }
  }

  private def timestampLiteral(l: ir.Literal) = {
    l.timestamp match {
      case Some(timestamp) =>
        singleQuote(
          LocalDateTime
            .from(ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.of("UTC")))
            .format(timeFormat))
      case None => "NULL"
    }
  }

  private def dateLiteral(l: ir.Literal) = {
    l.date match {
      case Some(date) =>
        singleQuote(
          LocalDate.from(ZonedDateTime.ofInstant(Instant.ofEpochMilli(date), ZoneId.of("UTC"))).format(dateFormat))
      case None => "NULL"
    }
  }

  private def mapExpr(ctx: GeneratorContext)(map: ir.MapExpr): String = {
    val entries = map.keys.zip(map.values).map { case (key, value) =>
      s"${literal(ctx, key)}, ${expression(ctx, value)}"
    }
    // TODO: line-width formatting
    s"MAP(${entries.mkString(", ")})"
  }

  private def arrayExpr(ctx: GeneratorContext)(array: ir.ArrayExpr): String = {
    val elements = array.elements.map { element =>
      expression(ctx, element)
    }
    // TODO: line-width formatting
    s"ARRAY(${elements.mkString(", ")})"
  }

  private def id(ctx: GeneratorContext, id: ir.Id): String = {
    if (id.caseSensitive) {
      doubleQuote(id.id)
    } else {
      id.id
    }
  }

  private def alias(ctx: GeneratorContext, alias: ir.Alias): String = {
    s"${expression(ctx, alias.expr)} AS ${alias.name.map(expression(ctx, _)).mkString(".")}"
  }

  private def distinct(ctx: GeneratorContext, distinct: ir.Distinct): String = {
    s"DISTINCT ${expression(ctx, distinct.expression)}"
  }

  private def star(ctx: GeneratorContext, star: ir.Star): String = {
    val objectRef = star.objectName.map(or => generateObjectReference(ctx, or) + ".").getOrElse("")
    s"$objectRef*"
  }

  private def generateObjectReference(ctx: GeneratorContext, reference: ir.ObjectReference): String = {
    (reference.head +: reference.tail).map(id(ctx, _)).mkString(".")
  }

  private def dot(ctx: GeneratorContext, dot: ir.Dot): String = {
    s"${expression(ctx, dot.left)}.${expression(ctx, dot.right)}"
  }

  private def objectReference(ctx: GeneratorContext, objRef: ir.ObjectReference): String = {
    (objRef.head +: objRef.tail).map(id(ctx, _)).mkString(".")
  }
  private def column(ctx: GeneratorContext, col: ir.Column): String = {
    val objRef = col.tableNameOrAlias.map(t => expression(ctx, t) + ".").getOrElse("")
    s"$objRef${id(ctx, col.columnName)}"
  }
  private def orNull(option: Option[String]): String = option.getOrElse("NULL")

  private def doubleQuote(s: String): String = s""""$s""""

  private def singleQuote(s: String): String = s"'$s'"
}
