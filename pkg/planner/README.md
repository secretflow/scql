## Example planner usage.
### There are two execution paths in tidb build physical plan from ast node.
1. AST -> Physical Plan Step-by-step through multiple functions
```
import (
    plannercore "github.com/pingcap/tidb/planner/core"
)

func main() {
    stmts, _, err := parser.Parse(SQL_STRING)
    is := getInfoSchema()
    logical_plan, _, err := plannercore.BuildLogicalPlan(stmts[0], is)
    physical_plan, err = plannercore.DoOptimize(
            context.TODO(),
            flagPredicatePushDown|flagDecorrelate|flagPrunColumns,
            logical_plan.(LogicalPlan))
}
```
2. AST -> Physical Plan in one function (AST -> logical plan -> optimize -> physical plan)
```
import (
    "github.com/pingcap/tidb/planner"
)

func main() {
    stmts, _, err := parser.Parse(SQL_STRING)
    is := getInfoSchema()
    physical_plan, _, err := planner.Optimize(
            context.TODO(),
            sessionctx.Context,
            stmts[0],
            is)
}
```

### So, there are several entry functions in planner.
1. In `planner/optimize.go`
```
func Optimize(
        ctx context.Context,
        sctx sessionctx.Context,
        node ast.Node,
        is infoschema.InfoSchema) (plannercore.Plan, types.NameSlice, error)
```
2. In `planner/core/planbuilder.go`
```
func (b *PlanBuilder) Build(
        ctx context.Context,
        node ast.Node) (Plan, error) // Returns LogicalPlan
```
3. In `planner/core/optimizer.go`
```
func DoOptimize(
        ctx context.Context,
        flag uint64,
        logic LogicalPlan) (PhysicalPlan, float64, error)
```
4. In `planner/core/optimizer.go`
```
func BuildLogicalPlan(
        ctx context.Context,
        sctx sessionctx.Context,
        node ast.Node,
        is infoschema.InfoSchema) (Plan, types.NameSlice, error) // Also returns LogicalPlan.
{ // Simply invokes Build() function above in 2 }
```
