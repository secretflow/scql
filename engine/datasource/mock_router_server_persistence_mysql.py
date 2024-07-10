#!/usr/bin/env python3

from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel
from jinja2 import Template
from fastapi.responses import HTMLResponse
from sqlalchemy import create_engine, Column, Integer, String, Text, ForeignKey, select
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
import uuid

DATABASE_URL_ASYNC = "mysql+aiomysql://root:123456@172.16.16.118:3307/datasource"


Base = declarative_base()

# 数据源ORM模型


class DataSourceORM(Base):
    __tablename__ = 'datasources'
    id = Column(String(80), primary_key=True)
    name = Column(String(50), nullable=False)
    kind = Column(String(80), nullable=False)
    connection_str = Column(Text, nullable=False)

# 路由规则ORM模型


class RouteRuleORM(Base):
    __tablename__ = 'route_rules'
    id = Column(Integer, primary_key=True, autoincrement=True)
    db = Column(String(50), nullable=False)
    table = Column(String(50), nullable=False)
    datasource_id = Column(String(80), ForeignKey(
        'datasources.id'), nullable=False)


# 初始化数据库连接
# engine = create_engine(DATABASE_URL)
# Base.metadata.create_all(bind=engine)
# SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
engine = create_async_engine(DATABASE_URL_ASYNC, echo=True)
SessionLocal = sessionmaker(
    engine, expire_on_commit=False, class_=AsyncSession)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


app = FastAPI()


class Table(BaseModel):
    db: str
    table: str


class RouteRequest(BaseModel):
    header: dict[str, str] | None = None
    tables: list[Table]


class Status(BaseModel):
    code: int = 0
    message: str = "ok"


class DataSource(BaseModel):
    id: str | None = None
    name: str
    kind: str
    connection_str: str


class RouteResponse(BaseModel):
    status: Status = Status()
    datasource_ids: list[str]
    datasources: dict[str, DataSource]


class RouteRule(BaseModel):
    db: str
    table: str
    datasource_id: str


# 修改MyStore类以使用依赖注入
class MyStore:
    def __init__(self):
        pass

    _counter = 0

    @classmethod
    async def _increment_counter(cls):
        cls._counter += 1
        return cls._counter

    # async def add_datasource(self, db: Session, ds: DataSource) -> DataSource:
    #     orm_ds = DataSourceORM(
    #         id=str(uuid.uuid4()),  # 为id分配一个新的UUID
    #         name=ds.name,
    #         kind=ds.kind,
    #         connection_str=ds.connection_str
    #     )
    #     db.add(orm_ds)
    #     try:
    #         await db.commit()
    #         db.refresh(orm_ds)
    #     except SQLAlchemyError as e:
    #         await db.rollback()
    #         raise HTTPException(status_code=500, detail=str(e))
    #     return DataSource(**orm_ds.__dict__)

    async def add_datasource(self, db: Session, ds: DataSource) -> DataSource:
        counter = await self._increment_counter()  # 获取并增加计数器
        orm_ds = DataSourceORM(
            id=f"ds{counter}",  # 使用累加的计数器生成id
            name=ds.name,
            kind=ds.kind,
            connection_str=ds.connection_str
        )
        db.add(orm_ds)
        try:
            await db.commit()
            db.refresh(orm_ds)
        except SQLAlchemyError as e:
            await db.rollback()
            raise HTTPException(status_code=500, detail=str(e))
        return DataSource(**orm_ds.__dict__)

    async def add_route_rule(self, db: Session, rule: RouteRule) -> None:
        orm_rule = RouteRuleORM(**rule.dict())
        db.add(orm_rule)
        try:
            await db.commit()
        except SQLAlchemyError as e:
            await db.rollback()
            raise HTTPException(status_code=500, detail=str(e))

    # 添加异步版本的get_datasource_by_id
    async def get_datasource_by_id(self, db: Session, ds_id: str) -> DataSourceORM | None:
        return db.query(DataSourceORM).filter_by(id=ds_id).first()

    # 添加异步版本的route方法
    # async def route(self, db: Session, dbName: str, tableName: str) -> str | None:
    #     rule = db.query(RouteRuleORM).filter_by(
    #         db=dbName, table=tableName).first()
    #     return rule.datasource_id if rule else None

    async def route(self, db: Session, dbName: str, tableName: str) -> str | None:

        rule = await db.scalar(
            select(RouteRuleORM).filter_by(db=dbName, table=tableName)
        )
        return rule.datasource_id if rule else None

        # 在MyStore类中添加获取数据源列表的方法

    async def get_datasource_list(self, db: Session) -> list[DataSourceORM]:
        return db.query(DataSourceORM).all()


store = MyStore()

# 更新FastAPI路由以使用异步函数和依赖注入


@app.post("/datasource/register")
async def register(ds: DataSource, db: Session = Depends(get_db)) -> DataSource:
    return await store.add_datasource(db=db, ds=ds)


@app.post("/datasource/route_rule")
async def add_route_rule(rule: RouteRule, db: Session = Depends(get_db)) -> None:
    await store.add_route_rule(db=db, rule=rule)


@app.post("/datasource/route")
async def route(req: RouteRequest, db: Session = Depends(get_db)) -> RouteResponse:
    if not req.tables:
        raise HTTPException(
            status_code=400, detail="Bad Request: Empty tables in request")

    dsList = []
    for tbl in req.tables:
        dsID = await store.route(db=db, dbName=tbl.db, tableName=tbl.table)
        if not dsID:
            raise HTTPException(status_code=404, detail="Route rule not found")
        dsList.append(dsID)

    # 确保查询也是异步的
    # datasources = {ds_id: DataSource(**ds.__dict__) async for ds_id, ds in (
    #     (ds.id, ds) async for ds in
    #     db.scalars(select(DataSourceORM).filter(DataSourceORM.id.in_(dsList)))
    # )}

    # 使用await来获取scalars的查询结果，得到一个异步迭代器
    query_result = await db.scalars(select(DataSourceORM).filter(DataSourceORM.id.in_(dsList)))

# # 现在你可以安全地使用async for进行迭代
#     datasources = {ds_id: DataSource(**ds.__dict__) async for ds_id, ds in (
#     (ds.id, ds) async for ds in query_result
# )}
    dataset = query_result.all()


# 现在可以安全地遍历结果列表
    datasources = {ds.id: DataSource(**ds.__dict__) for ds in dataset}

    return RouteResponse(datasource_ids=dsList, datasources=datasources)


@app.get("/", response_class=HTMLResponse)
async def index(db: Session = Depends(get_db)):
    # 获取数据源列表
    datasource_list = await store.get_datasource_list(db)

    # 注意：这里假设store类有相应的方法来获取routeRules等信息，如果没有，你需要定义它们
    # 或者直接在MyStore类中维护这些信息为属性，并提供获取方法

    # 构建渲染上下文
    context = {
        "datasources": {ds.id: DataSource(**ds.__dict__) for ds in datasource_list},
        # 假设其他属性有相应的方法或逻辑来获取
        # "tableRoutingRules": ...,
        # "dbRoutingRules": ...,
        # "defaultRoutingRule": ...
    }

    return indexTemplate.render(**context)

# ... (保留或调整原有的HTML响应部分和其他逻辑)

indexTemplateContent = """
<html>
<head>
  <title> Mock Router Service </title>
</head>
<body>
  <h1> Mock Router Service </h1>
  <p> NOTE: only used for debug/testing purpose, please do not use it in production environment!!! </p>
  <div class="datasource-list">
   <h1> Data Source List </h1>
   <table border="1">
    <tr>
      <td> id </td>
      <td> name </td>
      <td> kind </td>
      <td> connection string </td>
    </tr>
    {% for key, value in datasources.items() %}
      <tr>
        <td> {{ value.id }} </td>
        <td> {{ value.name }} </td>
        <td> {{ value.kind }} </td>
        <td> {{ value.connection_str }} </td>
      </tr>
    {% endfor %}
   </table>
  </div>
  <div class="routerule-list">
    <h1> Route Rule List </h1>
    <table border="1">
    <tr>
      <td> rule </td>
      <td> datasource id </td>
    </tr>
    {% for key, value in tableRoutingRules.items() %}
     <tr>
        <td> {{ key }} </td>
        <td> {{ value }} </td>
      </tr>
    {% endfor %}
    {% for key, value in dbRoutingRules.items() %}
     <tr>
        <td> {{ key }}.* </td>
        <td> {{ value }} </td>
      </tr>
    {% endfor %}
    {% if defaultRoutingRule != "" %}
      <tr>
        <td> *.* </td>
        <td> {{ defaultRoutingRule }} </td>
      </tr>
    {% endif %}
    </table>
  </div>
</body>
</html>
"""

indexTemplate = Template(indexTemplateContent)


@app.get("/", response_class=HTMLResponse)
def index():
    return indexTemplate.render(
        datasources=store.ds,
        tableRoutingRules=store.routeRules,
        dbRoutingRules=store.databaseRouteRules,
        defaultRoutingRule=store.defaultDataSourceID,
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="debug")
