from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
import sqlalchemy

Base = declarative_base()

class CitizenScienceBatches(Base):
    __tablename__ = 'citizen_science_batches'

    # Column defs
    cit_sci_batch_id = Column(Integer, primary_key=True)
    cit_sci_proj_id = Column(Integer, ForeignKey('citizen_science_projects.cit_sci_proj_id'))
    vendor_batch_id = Column(String(255))
    batch_status = Column(String(50))
    date_created = Column(DateTime)
    date_last_updates = Column(DateTime)

    def get_db_connection(db_host, db_port, db_name, db_user, db_pass):
            engine = sqlalchemy.create_engine("postgresql://{}:{}@{}:{}/{}".format(db_user, db_pass, db_host, db_port, db_name))
            engine.dialect.description_encoding = None
            Session = sessionmaker(bind=engine)
            return Session()