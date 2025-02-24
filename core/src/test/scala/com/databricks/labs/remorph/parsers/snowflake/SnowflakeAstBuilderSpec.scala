package com.databricks.labs.remorph.parsers
package snowflake

import com.databricks.labs.remorph.intermediate._
import com.databricks.labs.remorph.{intermediate => ir}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SnowflakeAstBuilderSpec
    extends AnyWordSpec
    with SnowflakeParserTestCommon
    with SetOperationBehaviors[SnowflakeParser]
    with Matchers
    with IRHelpers {

  override protected def astBuilder: SnowflakeAstBuilder = vc.astBuilder

  private def singleQueryExample(query: String, expectedAst: LogicalPlan): Unit =
    example(query, _.snowflakeFile(), Batch(Seq(expectedAst)))

  "SnowflakeAstBuilder" should {
    "translate a simple SELECT query" in {
      singleQueryExample(
        query = "SELECT a FROM TABLE",
        expectedAst = Project(NamedTable("TABLE", Map.empty), Seq(Id("a"))))
    }

    "translate a simple SELECT query with an aliased column" in {
      singleQueryExample(
        query = "SELECT a AS aa FROM b",
        expectedAst = Project(NamedTable("b", Map.empty), Seq(Alias(Id("a"), Id("aa")))))
    }

    "translate a simple SELECT query involving multiple columns" in {
      singleQueryExample(
        query = "SELECT a, b, c FROM table_x",
        expectedAst = Project(NamedTable("table_x", Map.empty), Seq(Id("a"), Id("b"), Id("c"))))
    }

    "translate a SELECT query involving multiple columns and aliases" in {
      singleQueryExample(
        query = "SELECT a, b AS bb, c FROM table_x",
        expectedAst = Project(NamedTable("table_x", Map.empty), Seq(Id("a"), Alias(Id("b"), Id("bb")), Id("c"))))
    }

    "translate a SELECT query involving a table alias" in {
      singleQueryExample(
        query = "SELECT t.a FROM table_x t",
        expectedAst = Project(TableAlias(NamedTable("table_x"), "t"), Seq(Dot(Id("t"), Id("a")))))
    }

    "translate a SELECT query involving a column alias and a table alias" in {
      singleQueryExample(
        query = "SELECT t.a, t.b as b FROM table_x t",
        expectedAst = Project(
          TableAlias(NamedTable("table_x"), "t"),
          Seq(Dot(Id("t"), Id("a")), Alias(Dot(Id("t"), Id("b")), Id("b")))))
    }

    val simpleJoinAst =
      Join(
        NamedTable("table_x", Map.empty),
        NamedTable("table_y", Map.empty),
        join_condition = None,
        UnspecifiedJoin,
        using_columns = Seq(),
        JoinDataType(is_left_struct = false, is_right_struct = false))

    "translate a query with a JOIN" in {
      singleQueryExample(
        query = "SELECT a FROM table_x JOIN table_y",
        expectedAst = Project(simpleJoinAst, Seq(Id("a"))))
    }

    "translate a query with a INNER JOIN" in {
      singleQueryExample(
        query = "SELECT a FROM table_x INNER JOIN table_y",
        expectedAst = Project(simpleJoinAst.copy(join_type = InnerJoin), Seq(Id("a"))))
    }

    "translate a query with a CROSS JOIN" in {
      singleQueryExample(
        query = "SELECT a FROM table_x CROSS JOIN table_y",
        expectedAst = Project(simpleJoinAst.copy(join_type = CrossJoin), Seq(Id("a"))))
    }

    "translate a query with a LEFT JOIN" in {
      singleQueryExample(
        query = "SELECT a FROM table_x LEFT JOIN table_y",
        expectedAst = Project(simpleJoinAst.copy(join_type = LeftOuterJoin), Seq(Id("a"))))
    }

    "translate a query with a LEFT OUTER JOIN" in {
      singleQueryExample(
        query = "SELECT a FROM table_x LEFT OUTER JOIN table_y",
        expectedAst = Project(simpleJoinAst.copy(join_type = LeftOuterJoin), Seq(Id("a"))))
    }

    "translate a query with a RIGHT JOIN" in {
      singleQueryExample(
        query = "SELECT a FROM table_x RIGHT JOIN table_y",
        expectedAst = Project(simpleJoinAst.copy(join_type = RightOuterJoin), Seq(Id("a"))))
    }

    "translate a query with a RIGHT OUTER JOIN" in {
      singleQueryExample(
        query = "SELECT a FROM table_x RIGHT OUTER JOIN table_y",
        expectedAst = Project(simpleJoinAst.copy(join_type = RightOuterJoin), Seq(Id("a"))))
    }

    "translate a query with a FULL JOIN" in {
      singleQueryExample(
        query = "SELECT a FROM table_x FULL JOIN table_y",
        expectedAst = Project(simpleJoinAst.copy(join_type = FullOuterJoin), Seq(Id("a"))))
    }

    "translate a query with a NATURAL JOIN" should {
      "SELECT a FROM table_x NATURAL JOIN table_y" in {
        singleQueryExample(
          query = "SELECT a FROM table_x NATURAL JOIN table_y",
          expectedAst = Project(simpleJoinAst.copy(join_type = NaturalJoin(UnspecifiedJoin)), Seq(Id("a"))))
      }
      "SELECT a FROM table_x NATURAL LEFT JOIN table_y" in {
        singleQueryExample(
          query = "SELECT a FROM table_x NATURAL LEFT JOIN table_y",
          expectedAst = Project(simpleJoinAst.copy(join_type = NaturalJoin(LeftOuterJoin)), Seq(Id("a"))))
      }
      "SELECT a FROM table_x NATURAL RIGHT JOIN table_y" in {
        singleQueryExample(
          query = "SELECT a FROM table_x NATURAL RIGHT JOIN table_y",
          expectedAst = Project(simpleJoinAst.copy(join_type = NaturalJoin(RightOuterJoin)), Seq(Id("a"))))
      }
    }

    "translate a query with a simple WHERE clause" in {
      val expectedOperatorTranslations = List(
        "=" -> Equals(Id("a"), Id("b")),
        "!=" -> NotEquals(Id("a"), Id("b")),
        "<>" -> NotEquals(Id("a"), Id("b")),
        ">" -> GreaterThan(Id("a"), Id("b")),
        "<" -> LessThan(Id("a"), Id("b")),
        ">=" -> GreaterThanOrEqual(Id("a"), Id("b")),
        "<=" -> LessThanOrEqual(Id("a"), Id("b")))

      expectedOperatorTranslations.foreach { case (op, expectedPredicate) =>
        singleQueryExample(
          query = s"SELECT a, b FROM c WHERE a $op b",
          expectedAst = Project(Filter(NamedTable("c", Map.empty), expectedPredicate), Seq(Id("a"), Id("b"))))
      }
    }

    "translate a query with a WHERE clause involving composite predicates" should {
      "SELECT a, b FROM c WHERE a = b AND b = a" in {
        singleQueryExample(
          query = "SELECT a, b FROM c WHERE a = b AND b = a",
          expectedAst = Project(
            Filter(NamedTable("c", Map.empty), And(Equals(Id("a"), Id("b")), Equals(Id("b"), Id("a")))),
            Seq(Id("a"), Id("b"))))
      }
      "SELECT a, b FROM c WHERE a = b OR b = a" in {
        singleQueryExample(
          query = "SELECT a, b FROM c WHERE a = b OR b = a",
          expectedAst = Project(
            Filter(NamedTable("c", Map.empty), Or(Equals(Id("a"), Id("b")), Equals(Id("b"), Id("a")))),
            Seq(Id("a"), Id("b"))))
      }
      "SELECT a, b FROM c WHERE NOT a = b" in {
        singleQueryExample(
          query = "SELECT a, b FROM c WHERE NOT a = b",
          expectedAst =
            Project(Filter(NamedTable("c", Map.empty), Not(Equals(Id("a"), Id("b")))), Seq(Id("a"), Id("b"))))
      }
    }

    "translate a query with a GROUP BY clause" in {
      singleQueryExample(
        query = "SELECT a, COUNT(b) FROM c GROUP BY a",
        expectedAst = Project(
          Aggregate(
            child = NamedTable("c", Map.empty),
            group_type = GroupBy,
            grouping_expressions = Seq(simplyNamedColumn("a")),
            pivot = None),
          Seq(Id("a"), CallFunction("COUNT", Seq(Id("b"))))))
    }

    "translate a query with a GROUP BY and ORDER BY clauses" in {
      singleQueryExample(
        query = "SELECT a, COUNT(b) FROM c GROUP BY a ORDER BY a",
        expectedAst = Project(
          Sort(
            Aggregate(
              child = NamedTable("c", Map.empty),
              group_type = GroupBy,
              grouping_expressions = Seq(simplyNamedColumn("a")),
              pivot = None),
            Seq(SortOrder(Id("a"), Ascending, NullsLast))),
          Seq(Id("a"), CallFunction("COUNT", Seq(Id("b"))))))
    }

    "translate a query with GROUP BY HAVING clause" in {
      singleQueryExample(
        query = "SELECT a, COUNT(b) FROM c GROUP BY a HAVING COUNT(b) > 1",
        expectedAst = Project(
          Filter(
            Aggregate(
              child = NamedTable("c", Map.empty),
              group_type = GroupBy,
              grouping_expressions = Seq(simplyNamedColumn("a")),
              pivot = None),
            GreaterThan(CallFunction("COUNT", Seq(Id("b"))), Literal(1))),
          Seq(Id("a"), CallFunction("COUNT", Seq(Id("b"))))))
    }

    "translate a query with ORDER BY" should {
      "SELECT a FROM b ORDER BY a" in {
        singleQueryExample(
          query = "SELECT a FROM b ORDER BY a",
          expectedAst =
            Project(Sort(NamedTable("b", Map.empty), Seq(SortOrder(Id("a"), Ascending, NullsLast))), Seq(Id("a"))))
      }
      "SELECT a FROM b ORDER BY a DESC" in {
        singleQueryExample(
          "SELECT a FROM b ORDER BY a DESC",
          Project(Sort(NamedTable("b", Map.empty), Seq(SortOrder(Id("a"), Descending, NullsFirst))), Seq(Id("a"))))
      }
      "SELECT a FROM b ORDER BY a NULLS FIRST" in {
        singleQueryExample(
          query = "SELECT a FROM b ORDER BY a NULLS FIRST",
          expectedAst =
            Project(Sort(NamedTable("b", Map.empty), Seq(SortOrder(Id("a"), Ascending, NullsFirst))), Seq(Id("a"))))
      }
      "SELECT a FROM b ORDER BY a DESC NULLS LAST" in {
        singleQueryExample(
          query = "SELECT a FROM b ORDER BY a DESC NULLS LAST",
          expectedAst =
            Project(Sort(NamedTable("b", Map.empty), Seq(SortOrder(Id("a"), Descending, NullsLast))), Seq(Id("a"))))
      }
    }

    "translate queries with LIMIT and OFFSET" should {
      "SELECT a FROM b LIMIT 5" in {
        singleQueryExample(
          query = "SELECT a FROM b LIMIT 5",
          expectedAst = Project(Limit(NamedTable("b", Map.empty), Literal(5)), Seq(Id("a"))))
      }
      "SELECT a FROM b LIMIT 5 OFFSET 10" in {
        singleQueryExample(
          query = "SELECT a FROM b LIMIT 5 OFFSET 10",
          expectedAst = Project(Offset(Limit(NamedTable("b", Map.empty), Literal(5)), Literal(10)), Seq(Id("a"))))
      }
      "SELECT a FROM b OFFSET 10 FETCH FIRST 42" in {
        singleQueryExample(
          query = "SELECT a FROM b OFFSET 10 FETCH FIRST 42",
          expectedAst = Project(Offset(NamedTable("b", Map.empty), Literal(10)), Seq(Id("a"))))
      }
    }

    "translate a query with PIVOT" in {
      singleQueryExample(
        query = "SELECT a FROM b PIVOT (SUM(a) FOR c IN ('foo', 'bar'))",
        expectedAst = Project(
          Aggregate(
            child = NamedTable("b", Map.empty),
            group_type = Pivot,
            grouping_expressions = Seq(CallFunction("SUM", Seq(simplyNamedColumn("a")))),
            pivot = Some(Pivot(simplyNamedColumn("c"), Seq(Literal("foo"), Literal("bar"))))),
          Seq(Id("a"))))
    }

    "translate a query with UNPIVOT" in {
      singleQueryExample(
        query = "SELECT a FROM b UNPIVOT (c FOR d IN (e, f))",
        expectedAst = Project(
          Unpivot(
            child = NamedTable("b", Map.empty),
            ids = Seq(simplyNamedColumn("e"), simplyNamedColumn("f")),
            values = None,
            variable_column_name = Id("c"),
            value_column_name = Id("d")),
          Seq(Id("a"))))
    }

    "translate queries with WITH clauses" should {
      "WITH a (b, c, d) AS (SELECT x, y, z FROM e) SELECT b, c, d FROM a" in {
        singleQueryExample(
          query = "WITH a (b, c, d) AS (SELECT x, y, z FROM e) SELECT b, c, d FROM a",
          expectedAst = WithCTE(
            Seq(
              SubqueryAlias(
                Project(namedTable("e"), Seq(Id("x"), Id("y"), Id("z"))),
                Id("a"),
                Seq(Id("b"), Id("c"), Id("d")))),
            Project(namedTable("a"), Seq(Id("b"), Id("c"), Id("d")))))
      }
      "WITH a (b, c, d) AS (SELECT x, y, z FROM e), aa (bb, cc) AS (SELECT xx, yy FROM f) SELECT b, c, d FROM a" in {
        singleQueryExample(
          query =
            "WITH a (b, c, d) AS (SELECT x, y, z FROM e), aa (bb, cc) AS (SELECT xx, yy FROM f) SELECT b, c, d FROM a",
          expectedAst = WithCTE(
            Seq(
              SubqueryAlias(
                Project(namedTable("e"), Seq(Id("x"), Id("y"), Id("z"))),
                Id("a"),
                Seq(Id("b"), Id("c"), Id("d"))),
              SubqueryAlias(Project(namedTable("f"), Seq(Id("xx"), Id("yy"))), Id("aa"), Seq(Id("bb"), Id("cc")))),
            Project(namedTable("a"), Seq(Id("b"), Id("c"), Id("d")))))
      }
      "WITH a (b, c, d) AS (SELECT x, y, z FROM e) SELECT x, y, z FROM e UNION SELECT b, c, d FROM a" in {
        singleQueryExample(
          query = "WITH a (b, c, d) AS (SELECT x, y, z FROM e) SELECT x, y, z FROM e UNION SELECT b, c, d FROM a",
          expectedAst = WithCTE(
            Seq(
              SubqueryAlias(
                Project(namedTable("e"), Seq(Id("x"), Id("y"), Id("z"))),
                Id("a"),
                Seq(Id("b"), Id("c"), Id("d")))),
            SetOperation(
              Project(namedTable("e"), Seq(Id("x"), Id("y"), Id("z"))),
              Project(namedTable("a"), Seq(Id("b"), Id("c"), Id("d"))),
              UnionSetOp,
              is_all = false,
              by_name = false,
              allow_missing_columns = false)))
      }
    }

    "translate a query with WHERE, GROUP BY, HAVING, QUALIFY" in {
      singleQueryExample(
        query = """SELECT c2, SUM(c3) OVER (PARTITION BY c2) as r
                  |  FROM t1
                  |  WHERE c3 < 4
                  |  GROUP BY c2, c3
                  |  HAVING AVG(c1) >= 5
                  |  QUALIFY MIN(r) > 6""".stripMargin,
        expectedAst = Project(
          Filter(
            Filter(
              Aggregate(
                child = Filter(namedTable("t1"), LessThan(Id("c3"), Literal(4))),
                group_type = GroupBy,
                grouping_expressions = Seq(simplyNamedColumn("c2"), simplyNamedColumn("c3")),
                pivot = None),
              GreaterThanOrEqual(CallFunction("AVG", Seq(Id("c1"))), Literal(5))),
            GreaterThan(CallFunction("MIN", Seq(Id("r"))), Literal(6))),
          Seq(Id("c2"), Alias(Window(CallFunction("SUM", Seq(Id("c3"))), Seq(Id("c2")), Seq(), None), Id("r")))))
    }

    behave like setOperationsAreTranslated(_.queryExpression())

    "translate Snowflake-specific set operators" should {
      "SELECT a FROM t1 MINUS SELECT b FROM t2" in {
        singleQueryExample(
          "SELECT a FROM t1 MINUS SELECT b FROM t2",
          SetOperation(
            Project(namedTable("t1"), Seq(Id("a"))),
            Project(namedTable("t2"), Seq(Id("b"))),
            ExceptSetOp,
            is_all = false,
            by_name = false,
            allow_missing_columns = false))
      }
      // Part of checking that UNION, EXCEPT and MINUS are processed with the same precedence: left-to-right
      "SELECT 1 UNION SELECT 2 EXCEPT SELECT 3 MINUS SELECT 4" should {
        singleQueryExample(
          "SELECT 1 UNION SELECT 2 EXCEPT SELECT 3 MINUS SELECT 4",
          SetOperation(
            SetOperation(
              SetOperation(
                Project(NoTable, Seq(Literal(1, IntegerType))),
                Project(NoTable, Seq(Literal(2, IntegerType))),
                UnionSetOp,
                is_all = false,
                by_name = false,
                allow_missing_columns = false),
              Project(NoTable, Seq(Literal(3, IntegerType))),
              ExceptSetOp,
              is_all = false,
              by_name = false,
              allow_missing_columns = false),
            Project(NoTable, Seq(Literal(4, IntegerType))),
            ExceptSetOp,
            is_all = false,
            by_name = false,
            allow_missing_columns = false))
      }
      "SELECT 1 UNION SELECT 2 EXCEPT SELECT 3 MINUS SELECT 4" should {
        singleQueryExample(
          "SELECT 1 UNION SELECT 2 EXCEPT SELECT 3 MINUS SELECT 4",
          SetOperation(
            SetOperation(
              SetOperation(
                Project(NoTable, Seq(Literal(1, IntegerType))),
                Project(NoTable, Seq(Literal(2, IntegerType))),
                UnionSetOp,
                is_all = false,
                by_name = false,
                allow_missing_columns = false),
              Project(NoTable, Seq(Literal(3, IntegerType))),
              ExceptSetOp,
              is_all = false,
              by_name = false,
              allow_missing_columns = false),
            Project(NoTable, Seq(Literal(4, IntegerType))),
            ExceptSetOp,
            is_all = false,
            by_name = false,
            allow_missing_columns = false))
      }
      "SELECT 1 EXCEPT SELECT 2 MINUS SELECT 3 UNION SELECT 4" should {
        singleQueryExample(
          "SELECT 1 EXCEPT SELECT 2 MINUS SELECT 3 UNION SELECT 4",
          SetOperation(
            SetOperation(
              SetOperation(
                Project(NoTable, Seq(Literal(1, IntegerType))),
                Project(NoTable, Seq(Literal(2, IntegerType))),
                ExceptSetOp,
                is_all = false,
                by_name = false,
                allow_missing_columns = false),
              Project(NoTable, Seq(Literal(3, IntegerType))),
              ExceptSetOp,
              is_all = false,
              by_name = false,
              allow_missing_columns = false),
            Project(NoTable, Seq(Literal(4, IntegerType))),
            UnionSetOp,
            is_all = false,
            by_name = false,
            allow_missing_columns = false))
      }
      "SELECT 1 MINUS SELECT 2 UNION SELECT 3 EXCEPT SELECT 4" should {
        singleQueryExample(
          "SELECT 1 MINUS SELECT 2 UNION SELECT 3 EXCEPT SELECT 4",
          SetOperation(
            SetOperation(
              SetOperation(
                Project(NoTable, Seq(Literal(1, IntegerType))),
                Project(NoTable, Seq(Literal(2, IntegerType))),
                ExceptSetOp,
                is_all = false,
                by_name = false,
                allow_missing_columns = false),
              Project(NoTable, Seq(Literal(3, IntegerType))),
              UnionSetOp,
              is_all = false,
              by_name = false,
              allow_missing_columns = false),
            Project(NoTable, Seq(Literal(4, IntegerType))),
            ExceptSetOp,
            is_all = false,
            by_name = false,
            allow_missing_columns = false))
      }
      // INTERSECT has higher precedence than UNION, EXCEPT and MINUS
      "SELECT 1 UNION SELECT 2 EXCEPT SELECT 3 INTERSECT SELECT 4" should {
        singleQueryExample(
          "SELECT 1 UNION SELECT 2 EXCEPT SELECT 3 MINUS SELECT 4 INTERSECT SELECT 5",
          ir.SetOperation(
            ir.SetOperation(
              ir.SetOperation(
                ir.Project(ir.NoTable, Seq(ir.Literal(1, ir.IntegerType))),
                ir.Project(ir.NoTable, Seq(ir.Literal(2, ir.IntegerType))),
                ir.UnionSetOp,
                is_all = false,
                by_name = false,
                allow_missing_columns = false),
              ir.Project(ir.NoTable, Seq(ir.Literal(3, ir.IntegerType))),
              ir.ExceptSetOp,
              is_all = false,
              by_name = false,
              allow_missing_columns = false),
            ir.SetOperation(
              ir.Project(ir.NoTable, Seq(ir.Literal(4, ir.IntegerType))),
              ir.Project(ir.NoTable, Seq(ir.Literal(5, ir.IntegerType))),
              ir.IntersectSetOp,
              is_all = false,
              by_name = false,
              allow_missing_columns = false),
            ir.ExceptSetOp,
            is_all = false,
            by_name = false,
            allow_missing_columns = false))
      }
    }

    "translate batches of queries" in {
      example(
        """
          |CREATE TABLE t1 (x VARCHAR);
          |SELECT x FROM t1;
          |SELECT 3 FROM t3;
          |""".stripMargin,
        _.snowflakeFile(),
        Batch(
          Seq(
            CreateTableCommand("t1", Seq(ColumnDeclaration("x", StringType))),
            Project(namedTable("t1"), Seq(Id("x"))),
            Project(namedTable("t3"), Seq(Literal(3))))))
    }

    // Tests below are just meant to verify that SnowflakeAstBuilder properly delegates DML commands
    // (other than SELECT) to SnowflakeDMLBuilder

    "translate INSERT commands" in {
      singleQueryExample(
        "INSERT INTO t (c1, c2, c3) VALUES (1,2, 3), (4, 5, 6)",
        InsertIntoTable(
          namedTable("t"),
          Some(Seq(Id("c1"), Id("c2"), Id("c3"))),
          Values(Seq(Seq(Literal(1), Literal(2), Literal(3)), Seq(Literal(4), Literal(5), Literal(6)))),
          None,
          None))
    }

    "translate DELETE commands" in {
      singleQueryExample(
        "DELETE FROM t WHERE t.c1 > 42",
        DeleteFromTable(namedTable("t"), None, Some(GreaterThan(Dot(Id("t"), Id("c1")), Literal(42))), None, None))
    }

    "translate UPDATE commands" in {
      singleQueryExample(
        "UPDATE t1 SET c1 = 42;",
        UpdateTable(namedTable("t1"), None, Seq(Assign(Column(None, Id("c1")), Literal(42))), None, None, None))
    }

    "survive an invalid command" in {
      example(
        """
          |CREATE TABLE t1 (x VARCHAR);
          |SELECT x y z;
          |SELECT 3 FROM t3;
          |""".stripMargin,
        _.snowflakeFile(),
        Batch(
          Seq(
            CreateTableCommand("t1", Seq(ColumnDeclaration("x", StringType))),
            UnresolvedRelation("Unparsable text: SELECTxyz", message = "Unparsed input - ErrorNode encountered"),
            UnresolvedRelation(
              """Unparsable text: SELECT
                |Unparsable text: x
                |Unparsable text: y
                |Unparsable text: z
                |Unparsable text: parser recovered by ignoring: SELECTxyz;""".stripMargin,
              message = "Unparsed input - ErrorNode encountered"),
            Project(namedTable("t3"), Seq(Literal(3))))),
        failOnErrors = false)

    }

    "translate BANG to Unresolved Expression" in {

      example(
        "!set error_flag = true;",
        _.snowSqlCommand(),
        UnresolvedCommand(
          ruleText = "!set error_flag = true;",
          ruleName = "snowSqlCommand",
          tokenName = Some("SQLCOMMAND"),
          message = "Unknown command in SnowflakeAstBuilder.visitSnowSqlCommand"))

      example(
        "!set dfsdfds",
        _.snowSqlCommand(),
        UnresolvedCommand(
          ruleText = "!set dfsdfds",
          ruleName = "snowSqlCommand",
          tokenName = Some("SQLCOMMAND"),
          message = "Unknown command in SnowflakeAstBuilder.visitSnowSqlCommand"))
      assertThrows[Exception] {
        example(
          "!",
          _.snowSqlCommand(),
          UnresolvedCommand(
            ruleText = "!",
            ruleName = "snowSqlCommand",
            tokenName = Some("SQLCOMMAND"),
            message = "Unknown command in SnowflakeAstBuilder.visitSnowSqlCommand"))
      }
      assertThrows[Exception] {
        example(
          "!badcommand",
          _.snowSqlCommand(),
          UnresolvedCommand(
            ruleText = "!badcommand",
            ruleName = "snowSqlCommand",
            tokenName = Some("SQLCOMMAND"),
            message = "Unknown command in SnowflakeAstBuilder.visitSqlCommand"))
      }
    }

    "translate amps" should {
      "select * from a where b = &ids" in {
        singleQueryExample(
          "select * from a where b = &ids",
          // Note when we truly process &vars we should get Variable, not Id
          Project(Filter(namedTable("a"), Equals(Id("b"), Id("$ids"))), Seq(Star())))
      }
    }

    "translate with recursive" should {
      "WITH RECURSIVE employee_hierarchy" in {
        singleQueryExample(
          """WITH RECURSIVE employee_hierarchy AS (
            |    SELECT
            |        employee_id,
            |        manager_id,
            |        employee_name,
            |        1 AS level
            |    FROM
            |        employees
            |    WHERE
            |        manager_id IS NULL
            |    UNION ALL
            |    SELECT
            |        e.employee_id,
            |        e.manager_id,
            |        e.employee_name,
            |        eh.level + 1 AS level
            |    FROM
            |        employees e
            |    INNER JOIN
            |        employee_hierarchy eh ON e.manager_id = eh.employee_id
            |)
            |SELECT *
            |FROM employee_hierarchy
            |ORDER BY level, employee_id;""".stripMargin,
          WithRecursiveCTE(
            Seq(
              SubqueryAlias(
                SetOperation(
                  Project(
                    Filter(NamedTable("employees"), IsNull(Id("manager_id"))),
                    Seq(
                      Id("employee_id"),
                      Id("manager_id"),
                      Id("employee_name"),
                      Alias(Literal(1, IntegerType), Id("level")))),
                  Project(
                    Join(
                      TableAlias(NamedTable("employees"), "e"),
                      TableAlias(NamedTable("employee_hierarchy"), "eh"),
                      join_condition = Some(Equals(Dot(Id("e"), Id("manager_id")), Dot(Id("eh"), Id("employee_id")))),
                      InnerJoin,
                      using_columns = Seq(),
                      JoinDataType(is_left_struct = false, is_right_struct = false)),
                    Seq(
                      Dot(Id("e"), Id("employee_id")),
                      Dot(Id("e"), Id("manager_id")),
                      Dot(Id("e"), Id("employee_name")),
                      Alias(Add(Dot(Id("eh"), Id("level")), Literal(1, IntegerType)), Id("level")))),
                  UnionSetOp,
                  is_all = true,
                  by_name = false,
                  allow_missing_columns = false),
                Id("employee_hierarchy"),
                Seq.empty)),
            Project(
              Sort(
                NamedTable("employee_hierarchy", Map.empty),
                Seq(SortOrder(Id("level"), Ascending, NullsLast), SortOrder(Id("employee_id"), Ascending, NullsLast))),
              Seq(Star(None)))))
      }
    }
  }
}
