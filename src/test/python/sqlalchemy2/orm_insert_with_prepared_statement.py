from sqlalchemy import insert
from sqlalchemy.orm import Session
from datetime import datetime, date
from connect import create_test_engine, generate_prepare_statement,\
  generate_execute_statement
from all_types import AllTypes

engine = create_test_engine()
with engine.connect() as conn:
  session = Session(engine)
  insert_stmt = insert(AllTypes).compile()
  prepare_stmt, params = generate_prepare_statement("insert_all_types",
                                                    insert_stmt)
  execute_stmt = generate_execute_statement("insert_all_types", insert_stmt)

  # Execute the `prepare <name> as <insert_stmt>` statement to create a prepared
  # statement.
  session.execute(prepare_stmt, params)

  row = AllTypes(
    col_bigint=1,
    col_bool=True,
    col_bytea=bytes("test bytes", "utf-8"),
    col_float8=3.14,
    col_int=100,
    col_numeric=6.626,
    col_timestamptz=datetime.fromisoformat("2011-11-04T00:05:23.123456+00:00"),
    col_date=date.fromisoformat("2011-11-04"),
    col_varchar="test string",
    col_jsonb={"key1": "value1", "key2": "value2"}
  )
  print(vars(row))
  # conn.execute(execute_stmt, vars(row))
  session.execute(execute_stmt, vars(row))
  # execute_stmt.bindparams(row)
  # conn.execute(execute_stmt, {"col_bigint": 1})
