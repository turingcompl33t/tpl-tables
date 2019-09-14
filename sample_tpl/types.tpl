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
    var pci = @tableIterGetPCI(&tvi)
    for (; @pciHasNext(pci); @pciAdvance(pci)) {
      out = @ptrCast(*output_struct, @outputAlloc(execCtx))
      out.col1 = @pciGetInt(pci, 0)
      out.col2 = @pciGetDouble(pci, 1) + @pciGetInt(pci, 0)
      out.col3 = @pciGetDate(pci, 2)
      if (@stringLike(@pciGetVarlen(pci, 3), pattern)) {
        out.col4 = @pciGetVarlen(pci, 3)
      } else {
        out.col4 = pattern
      }
    }
  }
  @outputFinalize(execCtx)
  @tableIterClose(&tvi)
  return 37
}
