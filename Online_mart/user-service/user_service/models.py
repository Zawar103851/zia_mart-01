# from typing import Optional
# from pydantic import Field
# from sqlmodel import SQLModel


# class User(SQLModel, table=True):
#     id: int | None = Field(default=None, primary_key=True)
#     name: str = Field()
#     email: str = Field()
#     password: str = Field()


# class Edit (SQLModel):
#     name: str = Field()
#     email: str = Field()
#     password:str = Field()
from typing import Optional
from sqlmodel import SQLModel, Field


class User(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)  # Primary key, optional on insert
    name: str = Field()  # Add max length constraint
    email: str = Field()  # Add max length constraint
    password: str = Field()  # Passwords are typically longer

class Edit(SQLModel):
    name: str = Field()
    email: str = Field()
    password: str = Field()
