from sqlmodel import SQLModel,Field


class Products(SQLModel,table = True):
    id : int = Field(default=None,primary_key=True)
    name : str = Field()
    price : int = Field()
    quantity : int = Field()