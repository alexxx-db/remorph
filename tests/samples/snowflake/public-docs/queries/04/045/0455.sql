-- see https://docs.snowflake.com/en/sql-reference/functions/regexp_instr

CREATE TABLE demo1 (id INT, string1 VARCHAR);
INSERT INTO demo1 (id, string1) VALUES 
    (1, 'nevermore1, nevermore2, nevermore3.')
    ;