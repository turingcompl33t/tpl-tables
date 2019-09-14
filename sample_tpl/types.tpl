struct output_struct {
  col1: Integer
  col2: Real
  col3: Date
  col4: StringVal
}

// SELECT colB, colC from test_1 WHERE colA < 500
fun main(execCtx: *ExecutionContext) -> int {
  var out : *output_struct
  var tvi: TableVectorIterator
  var pattern = @stringToSql("%d%")
  @tableIterInit(&tvi, "types1")
  for (@tableIterAdvance(&tvi)) {
    var pci = @tableIterGetVPI(&tvi)
    for (; @vpiHasNext(pci); @vpiAdvance(pci)) {
      out = @ptrCast(*output_struct, @outputAlloc(execCtx))
      out.col1 = @vpiGetInt(pci, 0)
      out.col2 = @vpiGetReal(pci, 1) + @vpiGetInt(pci, 0)
      out.col3 = @vpiGetDate(pci, 2)
      if (@stringLike(@vpiGetVarlen(pci, 3), pattern)) {
        out.col4 = @vpiGetVarlen(pci, 3)
      } else {
        out.col4 = pattern
      }
    }
  }
  @outputFinalize(execCtx)
  @tableIterClose(&tvi)
  return 37
}
