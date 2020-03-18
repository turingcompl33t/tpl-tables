struct State {
  sum: RealSumAggregate
}

fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
  @aggInit(&state.sum)
}

fun teardownState(execCtx: *ExecutionContext, state: *State) -> nil {
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
      var a = @pciGetVarlen(pci, 0)
      var b = @pciGetVarlen(pci, 1)
      var c = @pciGetDouble(pci, 2)
      var d = @pciGetDouble(pci, 3)
      var e = @pciGetDouble(pci, 4)
      var f = @pciGetDouble(pci, 5)
      var g = @pciGetInt(pci, 6)
      var h = @pciGetInt(pci, 7)
      var i = @pciGetInt(pci, 8)
      var j = @pciGetInt(pci, 9)
    }
  }

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
