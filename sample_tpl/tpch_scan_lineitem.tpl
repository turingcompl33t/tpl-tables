struct State {
  sum: RealSumAggregate
}

fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
  @aggInit(&state.sum)
}

fun teardownState(execCtx: *ExecutionContext, state: *State) -> nil {
}

struct outputStruct {
 c0 : StringVal
 c1 : StringVal
 c2 : Real
 c3 : Real
 c4 : Real
 c5 : Real
 c6 : Integer
 c7 : Integer
 c8 : Integer
 c9 : Integer
}

fun execQuery(execCtx: *ExecutionContext, state: *State) -> nil {
  @execCtxStartResourceTracker(execCtx, 3)
  // Pipeline 1 (hashing)
  var tvi: TableVectorIterator
  var oids : [10]uint32
  oids[0] = 9
  oids[1] = 10
  oids[2] = 5
  oids[3] = 6
  oids[4] = 7
  oids[5] = 8
  oids[6] = 1
  oids[7] = 2
  oids[8] = 3
  oids[9] = 4
  @tableIterInitBind(&tvi, execCtx, "lineitem", oids)
  for (@tableIterAdvance(&tvi)) {
    var pci = @tableIterGetPCI(&tvi)
    for (; @pciHasNext(pci); @pciAdvance(pci)) {
      var out = @ptrCast(*outputStruct, @outputAlloc(execCtx))
      out.c0 = @pciGetVarlen(pci, 0)
      out.c1 = @pciGetVarlen(pci, 1)
      out.c2 = @pciGetDouble(pci, 2)
      out.c3 = @pciGetDouble(pci, 3)
      out.c4 = @pciGetDouble(pci, 4)
      out.c5 = @pciGetDouble(pci, 5)
      out.c6 = @pciGetInt(pci, 6)
      out.c7 = @pciGetInt(pci, 7)
      out.c8 = @pciGetInt(pci, 8)
      out.c9 = @pciGetInt(pci, 9)
    }
  }

  @outputFinalize(execCtx)
  @tableIterClose(&tvi)
  @execCtxEndResourceTracker(execCtx, @getParamString(execCtx, 0))
}

fun main(execCtx: *ExecutionContext) -> int32 {
  var state: State
  setUpState(execCtx, &state)
  execQuery(execCtx, &state)
  teardownState(execCtx, &state)
  return 37
}
