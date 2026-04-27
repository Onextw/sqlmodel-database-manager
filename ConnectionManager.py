import logging, inspect, os
from typing import List, Dict
from types import ModuleType 
from sqlalchemy import Engine
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr
from sqlmodel import SQLModel, create_engine, Session
from dotenv import load_dotenv


load_dotenv()

log = logging.getLogger(__name__)

class Connection(BaseModel):
    """Database connection details."""
    model_config = ConfigDict(arbitrary_types_allowed=True)
    url: str = Field(..., description="Database URI")
    _engine: Engine|None = PrivateAttr(default=None)
    models:List[ModuleType] = Field(default_factory=list, description="List of Python modules containing SQLModel models")
    echo: bool = Field(default=True, description="Flag to enable or disable logging of database activity")


    @property
    def engine(self) -> Engine:
        """Returns the SQLAlchemy engine, initializing it if it hasn't been already."""
        if self._engine is None:
            self._engine = create_engine(self.url, echo=self.echo)
        return self._engine
    
    @property
    def table_names(self) -> List[str]:
        """Dynamically retrieves the set of table names from the assigned Modules containing registered SQLModel models."""
        return [
            obj.__tablename__ # type: ignore
            for module in self.models
            for name, obj in inspect.getmembers(module)
            if inspect.isclass(obj) and issubclass(obj, SQLModel) and obj is not SQLModel
        ]

    @property
    def tables(self) -> list:
        """Dynamically retrieves the set of tables from the assigned Modules containing registered SQLModel models."""
        return [SQLModel.metadata.tables[table_name] for table_name in self.table_names]
    
    def create_tables(self,*args, **kwargs) -> None:
        SQLModel.metadata.create_all(self.engine, tables=self.tables, *args, **kwargs)

    def get_session(self):
        with Session(self.engine) as session:
            yield session


class Connections:
    """Manages multiple database connections."""
    def __init__(self, *args, **kwargs):
        for key, value in kwargs.items():
            if not isinstance(value, Connection):
                raise ValueError("Value must be an instance of Connection.")
            if "echo" in kwargs and value.echo is None:
                value.echo = kwargs["echo"]
            setattr(self, key, value)
        
    def __setattr__(self, name, value):
        if not isinstance(value, Connection):
            raise ValueError("Value must be an instance of Connection.")
        super().__setattr__(name, value)

    def __setitem__(self, name, value):
        if not isinstance(value, Connection):
            raise ValueError("Value must be an instance of Connection.")
        setattr(self, name, value)

    def __getattr__(self, name) -> Connection:
        if name in self.__dict__:
            return self.__dict__[name]
        raise AttributeError(f"'Connections' object has no attribute '{name}'")

    def __getitem__(self, name) -> Connection:
        if name in self.__dict__:
            return self.__dict__[name]
        raise KeyError(f"'Connections' object has no connection named '{name}'")
    
    def create_all_tables(self) -> None:
        """Creates tables for all connections."""
        for connection in self.__dict__.values():
            if isinstance(connection, Connection):
                connection.create_tables()

    @property
    def connection_names(self) -> List[str]:
        """Returns a list of all connection names."""
        return [key for key, value in self.__dict__.items() if isinstance(value, Connection)]

    @property
    def connections(self) -> Dict[str, Connection]:
        """Returns a dictionary of connection names and their corresponding Connection instances."""
        return {key: value for key, value in self.__dict__.items() if isinstance(value, Connection)}

    @property
    def urls(self) -> Dict[str, str]:
        """Returns a dictionary of connection names and their corresponding URLs."""
        return {key: getattr(self, key).url for key in self.__dict__ if isinstance(getattr(self, key), Connection)}
    
    @property
    def table_names(self) -> Dict[str, list]:
        """Returns a dictionary of connection names and their corresponding table names."""
        return {key: getattr(self, key).table_names for key in self.__dict__ if isinstance(getattr(self, key), Connection)}

if __name__ == "__main__":
    db_manager = Connections() 
    db_manager.sample = Connection(
        url=os.getenv("DATABASE_URL", "sqlite:///./sample.db")
    )
    db_manager.sample.create_tables() 
    db_manager.create_all_tables()
    print(f"Connections: {db_manager.connection_names}")
    print(f"URLs: {db_manager.urls}")
    print(f"Table Names: {db_manager.table_names}")
