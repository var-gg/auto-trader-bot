from sqlalchemy import MetaData
from sqlalchemy.orm import declarative_base


research_metadata = MetaData()
ResearchBase = declarative_base(metadata=research_metadata)
