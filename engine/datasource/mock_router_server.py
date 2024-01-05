#!/usr/bin/env python3

from fastapi import FastAPI
from pydantic import BaseModel
from jinja2 import Template
from fastapi.responses import HTMLResponse


class Table(BaseModel):
    db: str
    table: str


class RouteRequest(BaseModel):
    header: dict[str, str] | None
    tables: list[Table]


class Status(BaseModel):
    code: int = 0
    message: str = "ok"


class DataSource(BaseModel):
    id: str | None
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


class MyStore:
    def __init__(self):
        self.ds = dict()
        # mapping `dbName.tableName` to datasource ID
        self.routeRules = dict()
        # mapping `dbName.*` to datasource IDs
        self.databaseRouteRules = dict()
        self.defaultDataSourceID = ""
        self.idx = 0

    # return added datasource with new id
    def add_datasource(self, ds):
        ds.id = "ds_{id}".format(id=self.idx)
        self.idx += 1
        self.ds[ds.id] = ds
        return ds

    def add_route_rule(self, dbName, tableName, dsID):
        if dbName == "*" and tableName == "*":
            # overwrite default datasource if exists
            self.defaultDataSourceID = dsID
        elif tableName == "*":
            self.databaseRouteRules[dbName] = dsID
        else:
            self.routeRules["{}.{}".format(dbName, tableName)] = dsID

    def route(self, dbName, tableName):
        if f"{dbName}.{tableName}" in self.routeRules.keys():
            return self.routeRules[f"{dbName}.{tableName}"]
        elif dbName in self.databaseRouteRules.keys():
            return self.databaseRouteRules[dbName]
        elif self.defaultDataSourceID != "":
            return self.defaultDataSourceID
        return None


store = MyStore()
app = FastAPI()


@app.post("/datasource/route")
def route(req: RouteRequest) -> RouteResponse:
    if req.tables is None or len(req.tables) == 0:
        return RouteResponse(
            status=Status(code=100, message="bad request: empty tables in request")
        )

    dsList = list()
    for tbl in req.tables:
        dsID = store.route(tbl.db, tbl.table)
        if dsID is None:
            return RouteResponse(
                status=Status(code=140, message="route rule not found")
            )
        else:
            dsList.append(dsID)

    datasources = dict()
    for dsID in dsList:
        if dsID in store.ds.keys():
            datasources[dsID] = store.ds[dsID]
        else:
            return RouteResponse(
                status=Status(code=141, message="datasource not found")
            )

    return RouteResponse(
        status=Status(code=0, message="ok"),
        datasource_ids=dsList,
        datasources=datasources,
    )


@app.post("/datasource/register")
def register(ds: DataSource):
    return store.add_datasource(ds)


@app.post("/datasource/route_rule")
def add_route_rule(rule: RouteRule):
    return store.add_route_rule(rule.db, rule.table, rule.datasource_id)


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
