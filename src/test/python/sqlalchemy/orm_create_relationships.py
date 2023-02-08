from sqlalchemy.orm import Session
from connect import create_test_engine
from user_model import User, Address

engine = create_test_engine()
session = Session(engine)
u1 = User(name="pkrabs", fullname="Pearl Krabs")
print(u1.addresses)

a1 = Address(email_address="pearl.krabs@gmail.com")
u1.addresses.append(a1)
print(u1.addresses)
print(a1.user)

a2 = Address(email_address="pearl@aol.com", user=u1)
print(u1.addresses)

session.add(u1)
print(u1 in session)
print(a1 in session)
print(a2 in session)

print(u1.id)
print(a1.user_id)
print(a2.user_id)

session.commit()
