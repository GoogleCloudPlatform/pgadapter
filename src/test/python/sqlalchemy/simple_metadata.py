from connect import create_test_engine
from user_metadata import user_table
from user_model import User, Address, mapper_registry

print(user_table.c.name)
print(user_table.c.keys())
print(user_table.primary_key)

print(User.__table__)
print(Address.__table__)

engine = create_test_engine(options="?options=-c spanner.ddl_transaction_mode"
                                    "=AutocommitExplicitTransaction")
mapper_registry.metadata.create_all(engine)