--
-- Verify the precedence rules are being correctly handled. Order of evaluation when chaining is:
-- 1. Brackets.
-- 2. INTERSECT
-- 3. UNION and EXCEPT, evaluated left to right.
--

-- tsql sql:

-- Verifies UNION/EXCEPT as left-to-right, with brackets.
<<<<<<< HEAD
(SELECT 1
 UNION
 SELECT 2
 EXCEPT
 (SELECT 3
  UNION
  SELECT 4))

UNION ALL

-- Verifies UNION/EXCEPT as left-to-right when the order is reversed.
(SELECT 5
 EXCEPT
 SELECT 6
 UNION
 SELECT 7)

UNION ALL

-- Verifies that INTERSECT has precedence over UNION/EXCEPT.
(SELECT 8
 UNION
 SELECT 9
 EXCEPT
 SELECT 10
 INTERSECT
 SELECT 11)

UNION ALL

-- Verifies that INTERSECT is left-to-right, although brackets have precedence.
(SELECT 12
 INTERSECT
 SELECT 13
 INTERSECT
 (SELECT 14
  INTERSECT
  SELECT 15));

-- databricks sql:

=======
SELECT 1
UNION
SELECT 2
EXCEPT
(SELECT 3
 UNION ALL
 SELECT 4)

INTERSECT

-- Verifies UNION/EXCEPT as left-to-right when the order is reversed.
SELECT 5
EXCEPT
SELECT 6
UNION
SELECT 7

INTERSECT

-- Verifies brackets have precedence over INTERSECT.
(SELECT 8
 INTERSECT
 SELECT 9)

INTERSECT

-- Not needed: just confirms helps us confirm that INTERSECT is also left-to-right.
SELECT 10;


-- databricks sql:
>>>>>>> a5bbdb69 (Implement remaining TSQL set operations. (#1227))
    (
        (
            (
                ((SELECT 1) UNION (SELECT 2))
            EXCEPT
<<<<<<< HEAD
                ((SELECT 3) UNION (SELECT 4))
            )
        UNION ALL
=======
                ((SELECT 3) UNION ALL (SELECT 4))
            )
        INTERSECT
>>>>>>> a5bbdb69 (Implement remaining TSQL set operations. (#1227))
            (
                ((SELECT 5) EXCEPT (SELECT 6))
            UNION
                (SELECT 7)
            )
        )
<<<<<<< HEAD
    UNION ALL
        (
            ((SELECT 8) UNION (SELECT 9))
        EXCEPT
            ((SELECT 10) INTERSECT (SELECT 11))
        )
    )
UNION ALL
    (
        ((SELECT 12) INTERSECT (SELECT 13))
    INTERSECT
        ((SELECT 14) INTERSECT (SELECT 15))
    );
=======
    INTERSECT
        ((SELECT 8) INTERSECT (SELECT 9))
    )
INTERSECT
    (SELECT 10);
>>>>>>> a5bbdb69 (Implement remaining TSQL set operations. (#1227))
