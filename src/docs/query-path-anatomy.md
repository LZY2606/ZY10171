# 查询路径解剖（Query Path Anatomy）

本文是一份**可执行**的 R-tree 查询说明：每一条结论都由
`QueryPathAnatomyTest` 中的确定性测试向前播放（forward play）验证，
而不是从最终结果集合倒推遍历过程。所有测试只依赖包私有（package-private）
的查询 observer 支架，不改变任何公开 API 与公开行为。

* 实现类：`OnSubscribeSearch` / `OnSubscribeSearch.SearchProducer`、
  `Backpressure`、`ObservedSearch`、`LeafHelper`、`NonLeafHelper`、
  `internal/operators/OperatorBoundedPriorityQueue`、`internal/Comparators`
* 测试支架（不进入生产 API）：`SearchObserver`（main 中，包私有抽象类）、
  测试侧 `SearchTrace`、`ProbeSubscriber`、`TraversalFixtures`、
  `OnSubscribeSearchAccess`
* 测试：[`QueryPathAnatomyTest`](../src/test/java/com/github/davidmoten/rtree/QueryPathAnatomyTest.java)

## 1. 参与部件与不可变性

树由三类节点构成：`Leaf`（持有 `Entry` 列表与自身 MBR）、`NonLeaf`
（持有子节点列表与子 MBR 的并集）、以及共同接口 `Node`。插入路径上的选择由
`Selector`（如 `SelectorMinimalOverlapArea`、`SelectorRStar`）决定，节点溢出由
`Splitter`（`SplitterQuadratic`、`SplitterRStar`）分裂。树是不可变的：
`add`/`delete` 只重建从根到被修改叶子的路径，
`internal/Util.replace` 用身份比较（`node != element`）原样保留未触及的子节点。

**结构共享与节点身份**（测试
`insertionRebuildsOnlyThePathAndSharesUnchangedSubtrees`）：一次插入后，
根是新身份，被修改路径上的节点是新身份，未触及的子树节点 `==` 旧节点。
遍历事件里记录的是节点身份，因此同一共享子树在新旧两个版本的查询中会以
**同一个 push 实例**出现。观察遍历只能依赖身份（`==`），不能依赖内容相等。

## 2. 一次查询的生命周期

`RTree.search(condition)` 在根存在时返回
`Observable.unsafeCreate(new OnSubscribeSearch(root, condition))`，根不存在时
返回 `Observable.empty()`（测试 `emptyTreeCompletesWithoutNodeEvents`）。
订阅时 `OnSubscribeSearch.call` 给 subscriber 安装一个 `SearchProducer`。

producer 内部状态：

* 一个 `ImmutableStack<NodePosition<T,S>>`，初始为 `[(root, 0)]`；
  `NodePosition` 是 `(node, position)`，`position` 是下一个要处理的子/条目下标。
* 一个 `AtomicLong requested` 累计未满足的需求。
* `node` 字段持有不可变根（与 `RTree` 共享），producer/subscription 存活期间
  该引用一直存在；它不是某次遍历的临时栈。

`request(long n)` 分两条路（`OnSubscribeSearch.SearchProducer`）：

* `n == Long.MAX_VALUE` 且当前无需求 → **快速路径** `requestAll()`，
  调用 `node.searchWithoutBackpressure(...)`（`LeafHelper.search` /
  `NonLeafHelper.search` 的递归实现）。
* 其它正数 → **背压路径** `requestSome(n)`，CAS 累加需求后由
  `Backpressure.search` 有界地驱动不可变栈。
* `n <= 0` → 本次调用直接返回，不碰栈、不终止订阅
  （测试 `requestZeroIsANoopThatIsRecorded`）。

## 3. 几何谓词怎样决定访问哪些节点（背压路径）

`Backpressure.searchAndReturnStack` 每次循环查看栈顶 `NodePosition`：

1. subscriber 已取消 → 返回空栈哨兵（产生 `cancel` 事件，见
   `cancelMidWalkStopsAllFurtherEvents`、`cancelBeforeRequestNeverWalks`）。
2. `request <= 0` → **带着当前栈立即返回**。这是“因为反压尚未访问”，
   不是几何裁剪。
3. `position == node.count()` → 该节点处理完毕，弹栈并把父节点位置前进一步
   （`searchAfterLastInNode`）。
4. 栈顶是 `NonLeaf` → 取 `child(position)`，对 **child 的 MBR** 调用一次
   condition：
   * true → `push((child, 0))`（事件 `push(child, depth)`），下降；
   * false → 弹出当前位置并推进（事件 `prune(child, depth)`），**该子树从未
     入栈、其条目永远不会被测试**。
5. 栈顶是 `Leaf` → 取 `entry(position)`，对**条目几何**调用 condition：
   * true → `subscriber.onNext(entry)`（事件 `hit`），需求减 1；
   * false → 只记录 `entryTested(matches=false)`，**不消耗需求**。
   无论命中与否，位置都前进一步。

关键区分（务必不要混淆）：

* **MBR 不可能命中（prune）**：在 NonLeaf 的下降决策点，由子节点 MBR 的谓词
  结果一次性排除整棵子树；被裁掉的节点从未进入栈。见
  `rectangleSearchPrunesWholeLeafByMbrAndEmitsInTreeOrder` 与
  `distanceBoundPrunesWholeSubtreeBeforeEntriesAreTested`。
* **尚未因反压访问（demand-gated pause）**：`request` 耗尽后，
  循环在栈顶带着未完成的栈返回。未被访问的叶子/条目**没有任何事件**，
  它们既不是 hit 也不是 prune。见
  `requestOneWalksTheTreeOneEmissionAtATime`、
  `missesDoNotConsumeDemandButZeroBudgetStopsBeforeNextDescent`、
  `repeatedRequestsAccumulateAndSingleCompletion`。

两个容易误判的实现细节（测试已锁定）：

* 背压路径**不对根做 MBR 预检**：根在 producer 构造时无条件入栈。
  根之外，下降时检查的是“子节点”的 MBR；叶子作为某个 NonLeaf 的子节点，
  其 MBR 在父节点的下降决策中已被检查（通过则 push 后逐个测试条目）。
* “零需求”检查位于循环**最顶端、在节点收尾（pop/前进）之前**。因此当一个
  hit 恰好耗尽最后一点需求时，即使已经是叶子里最后一个条目，pop/前进与
  后续兄弟的 MBR 裁剪也要等下一次 request 才发生
  （`requestOneWalksTheTreeOneEmissionAtATime` 的第三次 request 之后才出现
  `prune leafB` 与 `complete`）。
* 只有**真正发射的条目**（hit）扣减需求；miss 与 prune 都不扣减。

## 4. 几何谓词怎样决定访问哪些节点（快速路径）

`request(Long.MAX_VALUE)` 走 `LeafHelper.search` / `NonLeafHelper.search`：

* 进入**每个**节点（包括根）先对 `node.geometry().mbr()` 做一次门控，失败即
  返回；叶子门控通过后遍历全部条目；NonLeaf 门控通过后在相邻子节点之间只检查
  取消状态，然后递归进入子节点（子节点进入时再做自己的 MBR 门控）。

因此快速路径是**唯一可能观察到深度 0 的 prune（根 MBR 不通过）**的路径，
测试 `fastPathGatesRootMbrAndCanPruneAtDepthZero` 锁定了这一点；
`fastPathEmitsAllHitsInTreeOrder` 给出全命中时的完整访问序列。测试用的
`ObservedSearch` 逐行镜像上述 helper 语义并发出与背压路径相同的事件词汇；
生产代码在没有 observer 时仍然直接调用
`node.searchWithoutBackpressure(...)`，行为与性能不变。

## 5. Rectangle / Circle / Point 谓词

* `search(Rectangle)` → condition 为 `g -> g.intersects(r)`
  （`RTree.intersects`）。点的 MBR 是退化矩形，相交即包含，
  见 `pointSearchIsZeroAreaRectangleIntersection`。
* `search(Point)` → 直接委托 `search(point.mbr())`（`RTree.search(Point)`）。
* `search(Circle)` 与 `search(Line)` 是**两阶段**：
  先 `search(g.mbr())`（用外接矩形/MBR 走树），再用精确几何谓词做
  `.filter`（`RTree.search(R, Func2)` 与 `Intersects.geometryIntersectsCircle`）。
  因此遍历栈只按外接 MBR 决定下降：外接方盒内、圆盘外的条目是“MBR 候选但被
  精确过滤拒绝”，而**不是**被 prune。见
  `circleSearchReportsMbrCandidateThatExactFilterRejects` 与
  `pointInsideEnclosingSquareButOutsideDiskIsFilteredNotPruned`。

## 6. 距离上界（maxDistance）

`search(Rectangle, maxDistance)` 与 `search(Point, maxDistance)` 使用
**严格**谓词 `g.distance(r) < maxDistance`（`RTree.search(Rectangle,double)`）。
距离恰等於上界的条目不返回：`maxDistanceIsStrictAndPrunesByMbrDistance`
证明距离 2 在 `< 2` 下被排除。由于 condition 对 NonLeaf 子节点 MBR 同样成立
（`distance(g) < D` 是 MBR 安全谓词），子树 MBR 到查询几何的最小距离
`>= maxDistance` 时整棵子树在下降点被 prune，内部条目不做测试：
`distanceBoundPrunesWholeSubtreeBeforeEntriesAreTested`。

## 7. nearest 排序与 maxCount

`nearest(r, maxDistance, maxCount)` =
`search(r, maxDistance).lift(new OperatorBoundedPriorityQueue(maxCount,
Comparators.ascendingDistance(r)))`（`RTree.nearest`）。

* 上游仍是第 6 节的严格距离有界搜索；
  `maxCountCapsEmissionsButNotUpstreamVisits` 证明 maxCount **只限制队列保留/
  下游发射的条数，不限制上游对距离范围内候选的访问**。
* 队列在 `onCompleted` 时才用 `Comparators.ascendingDistance`
  （`internal/Comparators`，仅比较 `Double.compare(distance)`，无并列打破规则）
  排序并逐个发射。因此“按距离升序”是**队列排序的性质，不是树遍历顺序**，
  见 `nearestEmitsAscendingByDistanceRegardlessOfTreeOrder`、
  `nearestRespectsStrictMaxDistance`。
* **等距条目不承诺全局顺序**：`nearestEqualDistancesDoNotPromiseGlobalOrder`
  只断言更近者第一、两个并列距离条目都在集合中、maxCount 被遵守，
  从不断言两个并列条目的相对先后。
* 下游在队列发射有序列表期间取消，`OperatorBoundedPriorityQueue.call` 的
  onCompleted 循环在每个元素前检查 `isUnsubscribed()`，取消后不再 onNext：
  `boundedPriorityQueueStopsEmittingAfterUnsubscribe`。
  注意：对 `nearest` 而言，取消发生在“遍历完成后的发射阶段”，它阻止的是继续
  发射，并不会回退已经完成的上游遍历。

## 8. request、cancel 与错误的终止语义

* **request(1) 逐项取数**：见第 3 节的暂停点测试；重复 request 累加到同一个
  `AtomicLong`（`repeatedRequestsAccumulateAndSingleCompletion`），
  在 onNext 中重入 request(1) 会在同一次排空内继续消耗并最终只 complete 一次
  （`reentrantRequestFromOnNextDrainsAndCompletesOnce`）。
* **request(0)/负数**：记录但本次调用空操作，订阅保持可用，之后的正数 request
  照常工作（`requestZeroIsANoopThatIsRecorded`）。
* **cancel**：取消后下一次循环立即返回空栈并记录一次 `cancel`，不再产生任何
  hit/miss/prune/complete；取消发生在 request 之前时也一样
  （`cancelMidWalkStopsAllFurtherEvents`、`cancelBeforeRequestNeverWalks`）。
  **maxCount 与 cancel 之后都不会再产生 onNext**，由上述测试与
  `boundedPriorityQueueStopsEmittingAfterUnsubscribe` 共同保证。
* **subscriber 回调抛错**：condition 在测试条目时抛错，或 onNext 抛错，
  都由 `SearchProducer.request` 的同一个 catch 转发一次 `onError`
  （`predicateThrowDuringEntryTestTerminatesWithOnError`、
  `onNextThrowTerminatesRequestWithOnError`）。错误是终止信号：不 complete、
  同一次 request 不再发射第二个条目。

## 9. 订阅结束后遍历栈的树引用

* 背压路径栈排空后，producer 把 volatile `stack` 字段置为 `null`
  （`SearchProducer.requestSome`）；取消导致的空栈返回同样触发置空。
  因此订阅结束（正常完成或取消）后，遍历不再保留 `NodePosition` 链；
  见 `completedBackpressureWalkReleasesStackWhileRootRemainsShared` 与
  `cancelledWalkClearsStackAndRetainsNoPausedPosition`
  （通过包私有的 `SearchProducer.stackForTesting()` /
  `OnSubscribeSearchAccess` 直接断言字段为 null，不使用 GC/finalizer 或等待）。
* producer 的 `node` 字段仍持有不可变根，这是与 `RTree` **共享**的必要引用，
  其生命周期随 producer/subscription 一起结束，而不是每次遍历残留。
  暂停（尚未 complete 且未 cancel）期间栈保留暂停位置是必要的，不属于泄漏。

## 10. 复杂度

* `search`：平均 `O(log n)`，最坏 `O(n)`（全部 MBR 相交）。实际代价是
  “被 push/下降的节点数 + 被测试条目数”，每个被访问叶子条目一次谓词求值，
  每个候选子节点一次 MBR 谓词求值；`ImmutableStack` 的 push/pop 为
  `O(1)`，栈深 `O(h)`（h 为树高）。背压下每次 `requestSome` 只做与其新增
  发射量相关的工作，miss/prune 随遍历顺带发生。
* `nearest`：在有界搜索之上额外维护容量 `maxCount=k` 的
  `BoundedPriorityQueue`，插入 `O(log k)`，总计约
  `O(m log k)`（m 为 maxDistance 内被访问的候选数），额外空间 `O(k)`，
  发射在遍历完成后一次性按距离排序进行。

## 11. 兼容性与取舍

* 未新增/修改任何公开类型签名或公开行为；新增的 main 类
  （`SearchObserver`、`ObservedSearch`）与方法均为包私有，
  默认 observer 为无操作单例：无 observer 时快速路径仍调用
  `node.searchWithoutBackpressure(...)`，背压路径仍走原始
  `Backpressure.search(...)` 重载，生产路径无额外事件开销。
* 测试支架刻意放在与核心相同的包内以访问包私有成员，但它不暴露给外部使用者。
* 测试全部使用确定性小树与手构造节点（`TraversalFixtures`），
  不使用 `Thread.sleep`、不依赖外网、真实时钟等待或文件系统遍历顺序；
  失败时渲染完整的前向事件轨迹以便定位（`SearchTrace.render`）。
