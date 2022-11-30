from connect import create_test_engine
from model import mapper_registry


engine = create_test_engine(options="?options=-c spanner.ddl_transaction_mode"
                                    "=AutocommitExplicitTransaction")
mapper_registry.metadata.create_all(engine)
print("Created data model")
