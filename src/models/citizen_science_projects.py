from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Boolean
import sqlalchemy

class CitizenScienceProjects(Base):
    __tablename__ = 'citizen_science_batches'

    # Column defs
    cit_sci_proj_id = Column(Integer, primary_key=True)
    vendor_project_id = Column(Integer)
    owner_id = Column(Integer, ForeignKey('citizen_science_projects.cit_sci_owner_id'))
    project_status = Column(String(50))
    excess_data_exception = Column(Boolean)
    date_created = Column(DateTime)
    date_completed = Column(DateTime)
    excess_rights_approved = Column(Boolean)

    def get_db_connection(db_host, db_port, db_name, db_user, db_pass):
            engine = sqlalchemy.create_engine("postgresql://{}:{}@{}:{}/{}".format(db_user, db_pass, db_host, db_port, db_name))
            engine.dialect.description_encoding = None
            Session = sessionmaker(bind=engine)
            return Session()