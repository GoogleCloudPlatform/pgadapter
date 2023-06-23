from sqlalchemy.orm import Session
from connect import create_test_engine
from all_types import AllTypes


engine = create_test_engine()
session = Session(engine)
session.add_all([AllTypes(col_bigint=1, col_jsonb=None),
                 AllTypes(col_bigint=2, col_jsonb=None),
                 AllTypes(col_bigint=3, col_jsonb=None)])
session.commit()
print("Inserted 3 row(s)")
