from sqlalchemy import select
from sqlalchemy.orm import selectinload
from sqlalchemy.orm import Session
from connect import create_test_engine
from user_model import User


engine = create_test_engine()
session = Session(engine)

stmt = select(User).options(selectinload(User.addresses)).order_by(User.id)
for row in session.execute(stmt):
  print(f"{row.User.name}  "
        f"({', '.join(a.email_address for a in row.User.addresses)}) ")
