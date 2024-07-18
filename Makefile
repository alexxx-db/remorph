all: clean dev fmt lint test

clean:
	rm -fr .venv clean htmlcov .mypy_cache .pytest_cache .ruff_cache .coverage coverage.xml .python-version

setup_python:
	@echo "You have selected python setup with pyenv. It will install pyenv on your system."
	brew list pyenv &>/dev/null || brew install pyenv
	pyenv install -s 3.10
	pyenv local 3.10
	pyenv init - | grep PATH | tail -1
	@echo "Append the above line (export PATH...) to your profile i.e ~/.zshrc or ~/.bash_profile or ~/.profile and resource your profile for the changes in PATH variable to take effect."


dev:
	pip install hatch
	hatch env create
	hatch run pip install --upgrade pip
	hatch run pip install -e '.[test]'
	hatch run which python
	@echo "Hatch has created the above virtual environment. Please activate it using 'source .venv/bin/activate' and also select the .venv/bin/python interpreter in your IDE."


lint:
	hatch run verify

fmt: fmt-python fmt-scala

fmt-python:
	hatch run fmt

<<<<<<< HEAD
setup_spark_remote:
	.github/scripts/setup_spark_remote.sh

test:
	hatch run test

integration: setup_spark_remote
=======
fmt-scala:
	mvn -f pom.xml scalafmt:format

test: test-python test-scala

test-python:
	hatch run test

test-scala:
	mvn test -f pom.xml

integration:
>>>>>>> d5d174bf (Adding Maven and Scala build infra (#193))
	hatch run integration

coverage:
	hatch run coverage && open htmlcov/index.html

<<<<<<< HEAD
clean_coverage_dir:
	rm -fr ${OUTPUT_DIR}

python_coverage_report:
=======
build_core_jar:
	mvn --update-snapshots -B install -DskipTests -pl "!com.databricks.labs:remorph-coverage" --file pom.xml

dialect_coverage_report: build_core_jar
>>>>>>> 2705ee7d (Add snowflake coverage tests run to Makefile (#558))
	hatch run python src/databricks/labs/remorph/coverage/remorph_snow_transpilation_coverage.py
	hatch run pip install --upgrade sqlglot
	hatch -e sqlglot-latest run python src/databricks/labs/remorph/coverage/sqlglot_snow_transpilation_coverage.py
<<<<<<< HEAD
	hatch -e sqlglot-latest run python src/databricks/labs/remorph/coverage/sqlglot_tsql_transpilation_coverage.py

dialect_coverage_report: clean_coverage_dir python_coverage_report
	hatch run python src/databricks/labs/remorph/coverage/local_report.py
=======
	mvn compile -DskipTests exec:java -pl coverage --file pom.xml -DsourceDir=${INPUT_DIR} -DoutputPath=${OUTPUT_DIR} -DsourceDialect=Snow -Dextractor=full
>>>>>>> 2705ee7d (Add snowflake coverage tests run to Makefile (#558))
