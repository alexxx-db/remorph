-- snowflake sql:
SELECT s, t, EDITDISTANCE(s, t), EDITDISTANCE(t, s), EDITDISTANCE(s, t, 3), EDITDISTANCE(s, t, -1) FROM ed;

-- databricks sql:
<<<<<<< HEAD
SELECT s, t, LEVENSHTEIN(s, t), LEVENSHTEIN(t, s), LEVENSHTEIN(s, t, 3), LEVENSHTEIN(s, t, -1) FROM ed;
=======
SELECT s, t, LEVENSHTEIN(s, t), LEVENSHTEIN(t, s), LEVENSHTEIN(s, t, 3), LEVENSHTEIN(s, t, -1) FROM ed;
>>>>>>> 6de49dae (Improve coverage around snowflake functions (#860))
