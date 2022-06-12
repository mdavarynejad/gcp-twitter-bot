from sqlalchemy.ext.declarative import declarative_base
from config import read_config
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

Base = declarative_base()
cfg = read_config()
engine = create_engine(cfg['db']['path'])
Base.metadata.create_all(engine)
# Create an engine that stores data in the local directory's

class MakeSession(object):
    def __init__(self, db_path):
        self.engine = create_engine(db_path)
        # Bind the engine to the metadata of the Base class so that the
        # declaratives can be accessed through a DBSession instance
        Base.metadata.bind = self.engine
        DBSession = sessionmaker(bind=self.engine)
        self.session = DBSession()

def MakeSQLEnine():
    Base = declarative_base()
    engine = create_engine(cfg['db']['path'])
    Base.metadata.create_all(engine)
    return


def try_to_commit_to_db(db):
    try:
        db.commit()
        return_value = True
    except:
        db.rollback()
        return_value = False
    finally:
        db.close()

    return return_value
