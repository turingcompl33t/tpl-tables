struct Output {
  supp_nation: StringVal
  cust_nation: StringVal
  l_year : Integer
  volume : Real
}

struct DebugOutputStruct {
  d1 : Integer
  d2 : Integer
  d3 : Integer
  d4 : Integer
  d5 : Integer
  d6 : Integer
  d7 : Integer
  d8 : Integer
  d9 : Integer
  d10 : Integer
}

struct JoinRow1 {
  n1_nationkey : Integer
  n2_nationkey : Integer
  n1_name : StringVal
  n2_name : StringVal
}

struct JoinRow2 {
  n1_nationkey : Integer
  n1_name : StringVal
  n2_name : StringVal
  c_custkey : Integer
}

struct JoinRow3 {
  n1_nationkey : Integer
  n1_name : StringVal
  n2_name : StringVal
  o_orderkey : Integer
}

struct JoinRow4 {
  s_suppkey : Integer
  s_nationkey : Integer
}

struct JoinProbe4 {
  n1_nationkey : Integer
  l_suppkey : Integer
}

struct AggPayload {
  supp_nation: StringVal
  cust_nation: StringVal
  l_year : Integer
  volume : RealSumAggregate
}

struct AggValues {
  supp_nation: StringVal
  cust_nation: StringVal
  l_year : Integer
  volume : Real
}

struct SorterRow {
  supp_nation: StringVal
  cust_nation: StringVal
  l_year : Integer
  volume : Real
}

struct State {
  count: int32 // Debug
  join_table1: JoinHashTable
  join_table2: JoinHashTable
  join_table3: JoinHashTable
  join_table4: JoinHashTable
  agg_table: AggregationHashTable
  sorter: Sorter
}

// Check that the aggregate key already exists
fun checkAggKey(payload: *AggPayload, row: *AggValues) -> bool {
  if (payload.l_year != row.l_year) {
    return false
  }
  if (payload.supp_nation != row.supp_nation) {
    return false
  }
  if (payload.cust_nation != row.cust_nation) {
    return false
  }
  return true
}

// Sorter comparison function
fun sorterCompare(lhs: *SorterRow, rhs: *SorterRow) -> int32 {
  if (lhs.supp_nation < rhs.supp_nation) {
    return -1
  }
  if (lhs.supp_nation > rhs.supp_nation) {
    return 1
  }
  if (lhs.cust_nation < rhs.cust_nation) {
    return -1
  }
  if (lhs.cust_nation > rhs.cust_nation) {
    return 1
  }
  if (lhs.l_year < rhs.l_year) {
    return -1
  }
  if (lhs.l_year > rhs.l_year) {
    return 1
  }
  return 0
}


fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
  state.count = 0
  @joinHTInit(&state.join_table1, @execCtxGetMem(execCtx), @sizeOf(JoinRow1))
  @joinHTInit(&state.join_table2, @execCtxGetMem(execCtx), @sizeOf(JoinRow2))
  @joinHTInit(&state.join_table3, @execCtxGetMem(execCtx), @sizeOf(JoinRow3))
  @joinHTInit(&state.join_table4, @execCtxGetMem(execCtx), @sizeOf(JoinRow4))
  @aggHTInit(&state.agg_table, @execCtxGetMem(execCtx), @sizeOf(AggPayload))
  @sorterInit(&state.sorter, @execCtxGetMem(execCtx), sorterCompare, @sizeOf(SorterRow))
}

fun teardownState(execCtx: *ExecutionContext, state: *State) -> nil {
  @joinHTFree(&state.join_table1)
  @joinHTFree(&state.join_table2)
  @joinHTFree(&state.join_table3)
  @joinHTFree(&state.join_table4)
  @aggHTFree(&state.agg_table)
  @sorterFree(&state.sorter)
}

fun checkJoinKey1(execCtx: *ExecutionContext, probe: *ProjectedColumnsIterator, build: *JoinRow1) -> bool {
  // check c_nationkey == n2_nationkey
  if (@pciGetInt(probe, 1) != build.n2_nationkey) {
    return false
  }
  return true
}

fun checkJoinKey2(execCtx: *ExecutionContext, probe: *ProjectedColumnsIterator, build: *JoinRow2) -> bool {
  // o_custkey == c_custkey
  if (@pciGetInt(probe, 1) != build.c_custkey) {
    return false
  }
  return true
}

fun checkJoinKey3(execCtx: *ExecutionContext, probe: *ProjectedColumnsIterator, build: *JoinRow3) -> bool {
  // l_orderkey == o_orderkey
  if (@pciGetInt(probe, 2) != build.o_orderkey) {
    return false
  }
  return true
}

fun checkJoinKey4(execCtx: *ExecutionContext, probe: *JoinProbe4, build: *JoinRow4) -> bool {
  // l_suppkey == s_suppkey
  if (probe.l_suppkey != build.s_suppkey) {
    return false
  }
  // n1_nationkey == s_nationkey
  if (probe.n1_nationkey != build.s_nationkey) {
    return false
  }
  return true
}



// BNL nation with nation, then build JHT1
fun pipeline1(execCtx: *ExecutionContext, state: *State) -> nil {
  var n1_tvi : TableVectorIterator
  var n2_tvi : TableVectorIterator
  var france = @stringToSql("FRANCE")
  var germany = @stringToSql("GERMANY")
  // Step 1: Scan nation1
  var oids: [2]uint32
  oids[0] = 2 // n_name : varchar
  oids[1] = 1 // n_nationkey : int
  @tableIterInitBind(&n1_tvi, execCtx, "nation", oids)
  for (@tableIterAdvance(&n1_tvi)) {
    var vec1 = @tableIterGetPCI(&n1_tvi)
    for (; @pciHasNext(vec1); @pciAdvance(vec1)) {
      // n_name
      if (@pciGetVarlen(vec1, 0) == france or @pciGetVarlen(vec1, 0) == germany) {
        // Step 2: Scan nation2
        var oids2: [2]uint32
        oids2[0] = 2 // n_name : varchar
        oids2[1] = 1 // n_nationkey : int
        @tableIterInitBind(&n2_tvi, execCtx, "nation", oids2)
        for (@tableIterAdvance(&n2_tvi)) {
          var vec2 = @tableIterGetPCI(&n2_tvi)
          for (; @pciHasNext(vec2); @pciAdvance(vec2)) {
            if ((@pciGetVarlen(vec1, 0) == france and @pciGetVarlen(vec2, 0) == germany) or @pciGetVarlen(vec1, 0) == germany and @pciGetVarlen(vec2, 0) == france) {
              // Build JHT1
              var hash_val = @hash(@pciGetInt(vec2, 1)) // n2_nationkey
              var build_row1 = @ptrCast(*JoinRow1, @joinHTInsert(&state.join_table1, hash_val))
              build_row1.n1_nationkey = @pciGetInt(vec1, 1) // n1_nationkey
              build_row1.n2_nationkey = @pciGetInt(vec2, 1) // n2_nationkey
              build_row1.n1_name = @pciGetVarlen(vec1, 0) // n1_name
              build_row1.n2_name = @pciGetVarlen(vec2, 0) // n2_name
            }
          }
        }
        @tableIterClose(&n2_tvi)
      }
    }
  }
  // Build table
  @joinHTBuild(&state.join_table1)
  @tableIterClose(&n1_tvi)
}

// Scan Customer, probe JHT1, build JHT2
fun pipeline2(execCtx: *ExecutionContext, state: *State) -> nil {
  // Step1: Scan customer
  var c_tvi : TableVectorIterator
  var oids: [2]uint32
  oids[0] = 1 // c_custkey : int
  oids[1] = 4 // c_nationkey : int
  @tableIterInitBind(&c_tvi, execCtx, "customer", oids)
  for (@tableIterAdvance(&c_tvi)) {
    var vec = @tableIterGetPCI(&c_tvi)
    for (; @pciHasNext(vec); @pciAdvance(vec)) {
      // Step 2: Probe JHT1
      var hash_val = @hash(@pciGetInt(vec, 1)) // c_nationkey
      var hti: JoinHashTableIterator
      for (@joinHTIterInit(&hti, &state.join_table1, hash_val); @joinHTIterHasNext(&hti, checkJoinKey1, execCtx, vec);) {
        var join_row1 = @ptrCast(*JoinRow1, @joinHTIterGetRow(&hti))

        // Step 3: Insert into JHT2
        var hash_val2 = @hash(@pciGetInt(vec, 0)) // c_custkey
        var build_row2 = @ptrCast(*JoinRow2, @joinHTInsert(&state.join_table2, hash_val2))
        build_row2.n1_nationkey = join_row1.n1_nationkey
        build_row2.n1_name = join_row1.n1_name
        build_row2.n2_name = join_row1.n2_name
        build_row2.c_custkey = @pciGetInt(vec, 0) // c_custkey
      }
    }
  }
  // Build table
  @joinHTBuild(&state.join_table2)
  @tableIterClose(&c_tvi)
}

// Scan orders, probe JHT2, build JHT3
fun pipeline3(execCtx: *ExecutionContext, state: *State) -> nil {
  // Step 1: Sequential Scan
  var o_tvi : TableVectorIterator
  var oids: [2]uint32
  oids[0] = 1 // o_orderkey : int
  oids[1] = 2 // o_custkey : int
  @tableIterInitBind(&o_tvi, execCtx, "orders", oids)
  for (@tableIterAdvance(&o_tvi)) {
    var vec = @tableIterGetPCI(&o_tvi)
    for (; @pciHasNext(vec); @pciAdvance(vec)) {
      // Step 2: Probe JHT2
      var hash_val = @hash(@pciGetInt(vec, 1)) // o_custkey
      var hti: JoinHashTableIterator
      for (@joinHTIterInit(&hti, &state.join_table2, hash_val); @joinHTIterHasNext(&hti, checkJoinKey2, execCtx, vec);) {
        var join_row2 = @ptrCast(*JoinRow2, @joinHTIterGetRow(&hti))

        // Step 3: Insert into join table 3
        var hash_val3 = @hash(@pciGetInt(vec, 0)) // o_orderkey
        var build_row3 = @ptrCast(*JoinRow3, @joinHTInsert(&state.join_table3, hash_val3))
        build_row3.n1_nationkey = join_row2.n1_nationkey
        build_row3.n1_name = join_row2.n1_name
        build_row3.n2_name = join_row2.n2_name
        build_row3.o_orderkey = @pciGetInt(vec, 0)
      }
    }
  }
  // Build table
  @joinHTBuild(&state.join_table3)
  @tableIterClose(&o_tvi)
}

// Scan supplier, build JHT4
fun pipeline4(execCtx: *ExecutionContext, state: *State) -> nil {
  var s_tvi : TableVectorIterator
  var oids: [2]uint32
  oids[0] = 1 // s_suppkey : int
  oids[1] = 4 // s_nationkey : int
  @tableIterInitBind(&s_tvi, execCtx, "supplier", oids)
  for (@tableIterAdvance(&s_tvi)) {
    var vec = @tableIterGetPCI(&s_tvi)
    for (; @pciHasNext(vec); @pciAdvance(vec)) {
      var hash_val = @hash(@pciGetInt(vec, 0), @pciGetInt(vec, 1)) // s_suppkey, s_nationkey
      var build_row4 = @ptrCast(*JoinRow4, @joinHTInsert(&state.join_table4, hash_val))
      build_row4.s_suppkey = @pciGetInt(vec, 0) // s_suppkey
      build_row4.s_nationkey = @pciGetInt(vec, 1) // s_nationkey
    }
  }
  // Build table
  @joinHTBuild(&state.join_table4)
  @tableIterClose(&s_tvi)
}

// Scan lineitem, probe JHT3, probe JHT4, build AHT
fun pipeline5(execCtx: *ExecutionContext, state: *State) -> nil {
  var l_tvi : TableVectorIterator
  var oids: [5]uint32
  oids[0] = 6 // l_extendedprice : real
  oids[1] = 7 // l_discount : real
  oids[2] = 1 // l_orderkey : int
  oids[3] = 3 // l_suppkey : int
  oids[4] = 11 // l_shipdate : date
  @tableIterInitBind(&l_tvi, execCtx, "lineitem", oids)
  for (@tableIterAdvance(&l_tvi)) {
    var vec = @tableIterGetPCI(&l_tvi)
    for (; @pciHasNext(vec); @pciAdvance(vec)) {
      // l_shipdate
      if (@pciGetDate(vec, 4) >= @dateToSql(1995, 1, 1) and @pciGetDate(vec, 4) <= @dateToSql(1996, 12, 31)) {
        // Step 2: Probe JHT3
        var hash_val = @hash(@pciGetInt(vec, 2)) // l_orderkey
        var hti3: JoinHashTableIterator
        for (@joinHTIterInit(&hti3, &state.join_table3, hash_val); @joinHTIterHasNext(&hti3, checkJoinKey3, execCtx, vec);) {
          var join_row3 = @ptrCast(*JoinRow3, @joinHTIterGetRow(&hti3))

          // Step 3: Probe JHT4
          var hash_val4 = @hash(@pciGetInt(vec, 3), join_row3.n1_nationkey) // l_suppkey
          var join_probe4 : JoinProbe4 // Materialize the right pipeline
          join_probe4.l_suppkey = @pciGetInt(vec, 3)
          join_probe4.n1_nationkey = join_row3.n1_nationkey
          var hti4: JoinHashTableIterator
          for (@joinHTIterInit(&hti4, &state.join_table4, hash_val4); @joinHTIterHasNext(&hti4, checkJoinKey4, execCtx, &join_probe4);) {
            var join_row4 = @ptrCast(*JoinRow4, @joinHTIterGetRow(&hti4))

            // Step 4: Build Agg HT
            var agg_input : AggValues // Materialize
            agg_input.supp_nation = join_row3.n1_name
            agg_input.cust_nation = join_row3.n2_name
            agg_input.l_year = @extractYear(@pciGetDate(vec, 4))
            agg_input.volume = @pciGetDouble(vec, 0) * (1.0 - @pciGetDouble(vec, 1)) // l_extendedprice * (1.0 -  l_discount)
            var agg_hash_val = @hash(agg_input.supp_nation, agg_input.cust_nation, agg_input.l_year)
            var agg_payload = @ptrCast(*AggPayload, @aggHTLookup(&state.agg_table, agg_hash_val, checkAggKey, &agg_input))
            if (agg_payload == nil) {
              agg_payload = @ptrCast(*AggPayload, @aggHTInsert(&state.agg_table, agg_hash_val))
              agg_payload.supp_nation = agg_input.supp_nation
              agg_payload.cust_nation = agg_input.cust_nation
              agg_payload.l_year = agg_input.l_year
              @aggInit(&agg_payload.volume)
            }
            @aggAdvance(&agg_payload.volume, &agg_input.volume)
          }
        }
      }
    }
  }
  // Build table
  @tableIterClose(&l_tvi)
}

// Scan AHT, sort
fun pipeline6(execCtx: *ExecutionContext, state: *State) -> nil {
  var agg_ht_iter: AggregationHashTableIterator
  var agg_iter = &agg_ht_iter
  // Step 1: Iterate through Agg Hash Table
  for (@aggHTIterInit(agg_iter, &state.agg_table); @aggHTIterHasNext(agg_iter); @aggHTIterNext(agg_iter)) {
    var agg_payload = @ptrCast(*AggPayload, @aggHTIterGetRow(agg_iter))
    // Step 2: Build Sorter
    var sorter_row = @ptrCast(*SorterRow, @sorterInsert(&state.sorter))
    sorter_row.supp_nation = agg_payload.supp_nation
    sorter_row.cust_nation = agg_payload.cust_nation
    sorter_row.l_year = agg_payload.l_year
    sorter_row.volume = @aggResult(&agg_payload.volume)
  }
  @sorterSort(&state.sorter)
  @aggHTIterClose(agg_iter)
}

fun pipeline7(execCtx: *ExecutionContext, state: *State) -> nil {
  var sort_iter: SorterIterator
  for (@sorterIterInit(&sort_iter, &state.sorter); @sorterIterHasNext(&sort_iter); @sorterIterNext(&sort_iter)) {
    var out = @ptrCast(*Output, @outputAlloc(execCtx))
    var sorter_row = @ptrCast(*SorterRow, @sorterIterGetRow(&sort_iter))
    out.supp_nation = sorter_row.supp_nation
    out.cust_nation = sorter_row.cust_nation
    out.l_year = sorter_row.l_year
    out.volume = sorter_row.volume
    state.count = state.count + 1
  }
  @sorterIterClose(&sort_iter)
}

fun execQuery(execCtx: *ExecutionContext, state: *State) -> nil {
  @execCtxStartResourceTracker(execCtx, 3)
  pipeline1(execCtx, state)
  @execCtxEndResourceTracker(execCtx, @getParamString(execCtx, 0))
  @execCtxStartResourceTracker(execCtx, 3)
  pipeline2(execCtx, state)
  @execCtxEndResourceTracker(execCtx, @getParamString(execCtx, 1))
  @execCtxStartResourceTracker(execCtx, 3)
  pipeline3(execCtx, state)
  @execCtxEndResourceTracker(execCtx, @getParamString(execCtx, 2))
  @execCtxStartResourceTracker(execCtx, 3)
  pipeline4(execCtx, state)
  @execCtxEndResourceTracker(execCtx, @getParamString(execCtx, 3))
  @execCtxStartResourceTracker(execCtx, 3)
  pipeline5(execCtx, state)
  @execCtxEndResourceTracker(execCtx, @getParamString(execCtx, 4))
  @execCtxStartResourceTracker(execCtx, 3)
  pipeline6(execCtx, state)
  @execCtxEndResourceTracker(execCtx, @getParamString(execCtx, 5))
  @execCtxStartResourceTracker(execCtx, 3)
  pipeline7(execCtx, state)
  @execCtxEndResourceTracker(execCtx, @getParamString(execCtx, 6))
  @outputFinalize(execCtx)
}


fun main(execCtx: *ExecutionContext) -> int32 {
    var state: State
    setUpState(execCtx, &state)
    execQuery(execCtx, &state)
    teardownState(execCtx, &state)
    return state.count
}
