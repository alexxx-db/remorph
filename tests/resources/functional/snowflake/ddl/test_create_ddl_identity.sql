-- snowflake sql:

CREATE OR REPLACE TABLE sales_data (
    sale_id INT AUTOINCREMENT,
    product_id INT,
    quantity INT,
    sale_amount DECIMAL(10, 2),
    sale_date DATE,
    customer_id INT
);

-- databricks sql:
CREATE OR REPLACE TABLE sales_data (
    sale_id  DECIMAL(38, 0) GENERATED ALWAYS AS IDENTITY,
    product_id  DECIMAL(38, 0),
    quantity  DECIMAL(38, 0),
    sale_amount DECIMAL(10, 2),
    sale_date DATE,
    customer_id  DECIMAL(38, 0)
<<<<<<< HEAD
);
=======
<<<<<<< HEAD
<<<<<<< HEAD
);
=======
);
>>>>>>> b96aa6af (Create Command Extended (#1033))
=======
);
>>>>>>> 9ffc6a0d (EditorConfig setup for project (#1246))
>>>>>>> databrickslabs-main
