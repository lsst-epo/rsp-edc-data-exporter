from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, Integer, ForeignKey, BigInteger
import sqlalchemy

class CitizenScienceProjMetaLookup(Base):
    __tablename__ = 'citizen_science_proj_meta_lookup'

    # Column defs
    cit_sci_proj_id = Column(Integer, ForeignKey('citizen_science_projects.cit_sci_proj_id'))
    cit_sci_meta_id = Column(Integer, ForeignKey('citizen_science_meta.cit_sci_meta_id'))
    cit_sci_batch_id = Column(Integer, ForeignKey('citizen_science_batchs.cit_sci_batch_id'))

    def get_db_connection(db_host, db_port, db_name, db_user, db_pass):
            engine = sqlalchemy.create_engine("postgresql://{}:{}@{}:{}/{}".format(db_user, db_pass, db_host, db_port, db_name))
            engine.dialect.description_encoding = None
            Session = sessionmaker(bind=engine)
            return Session()