# R-tree 查询路径解剖（Query Path Anatomy）

本文档从前向（树结构、几何谓词、距离上界、subscriber 需求）解释一次 `search`/`nearest`
到底访问哪些节点，而**不是**从最终结果集合倒推遍历顺序。每条结论都链接到可执行测试
（`QueryPathTest`）与对应实现类。测试全部同步、确定性执行：不使用 `sleep`、真实时钟、
外网或文件系统遍历顺序。

- 测试：[QueryPathTest.java](../test/java/com/github/davidmoten/rtree/QueryPathTest.java)
- 确定性小树 fixture：[QueryPathFixtures.java](../test/java/com/github/davidmoten/rtree/QueryPathFixtures.java)
- 事件记录器：[SearchTrace.java](../test/java/com/github/davidmoten/rtree/SearchTrace.java)
- 包级私有 observer：[SearchObserver.java](../main/java/com/github/davidmoten/rtree/SearchObserver.java)

## 1. 两个遍历引擎与 subscriber 需求

订阅 `RTree.search(...)` 返回的 `Observable` 时会安装一个 producer：

- 实现：[`OnSubscribeSearch`](../main/java/com/github/davidmoten/rtree/OnSubscribeSearch.java)
  （内部类 `SearchProducer`）
- 有限需求（例如 `request(1)`）走**背压慢路径**，由一个不可变 `ImmutableStack<NodePosition>`
  驱动，实现为 [`Backpressure`](../main/java/com/github/davidmoten/rtree/Backpressure.java)。
- `request(Long.MAX_VALUE)` 走**无背压快路径**，递归实现位于
  [`NonLeafHelper`](../main/java/com/github/davidmoten/rtree/internal/NonLeafHelper.java)
  与 [`LeafHelper`](../main/java/com/github/davidmoten/rtree/internal/LeafHelper.java)；
  observer 对应的同构递归在 `OnSubscribeSearch.searchFast` 中。

**结论 1：订阅建立时根节点就已经在栈上，即使没有任何 demand；零与负需求不会产生任何访问。**
见测试 `testRectangleSearchPrunesDisjointLeafAndPausesOnDemand`（订阅后、request 前只有
`push(root)@0`）、`testZeroAndNegativeRequestsAreRecordedButNeverTraverse`
（`request(0)`/`request(-7)` 被记录但不触发访问）。

**结论 2：快路径与慢路径的*结构性*事件（push/prune/leafHit/complete）完全一致，区别只在
demand 信号的形状**（快路径是单个无界 request，慢路径是逐次有限 request）。
见 `testFastAndSlowPathsReportIdenticalStructuralEvents`。

> 注意：经过 RxJava `SafeSubscriber` 时，其 `onStart` 默认补发 `Long.MAX_VALUE`。
> 要确定性地观察逐项取数，测试直接构造 `SearchProducer`（见测试文件的 javadoc 与
> `producer(...)` 辅助方法）。这是测试支架的选择，不改变公开行为。

## 2. 几何谓词：MBR 不可能命中 ⇒ 整支裁剪

谓词对节点的 MBR 求值；谓词必须满足「entry 命中 ⇒ 所有祖先 MBR 命中」这一单调契约
（见 `RTree.search(Func1)` 的 javadoc）。于是：

- 非叶子节点的子节点 MBR 不满足谓词 ⇒ 该子节点及其全部后代**不会被访问**，记录为 `prune`。
  实现：`Backpressure.searchNonLeaf`。
- 叶子内的 entry 不满足谓词时只是**未命中**，不是裁剪：它不进入结果，也**不消耗需求单位**。
  实现：`Backpressure.searchLeaf`（`nextRequest = state.request - 1` 仅在命中时执行）。

矩形相交查询：[`RTree.intersects(Rectangle)`](../main/java/com/github/davidmoten/rtree/RTree.java)
委托给 `Geometry.intersects(Rectangle)`。

**结论 3：不相交叶子在 MBR 层被裁剪，叶子内不相交 entry 只是被静默跳过。**
见 `testRectangleSearchPrunesDisjointLeafAndPausesOnDemand`（`prune(leafB)` 与「b 被测试但
未命中」区分开）与 `testPointSearchPrunesNonContainingLeafFastPath`（point 搜索只进入
包含它的叶子）。

## 3. “尚未因反压访问” ≠ “MBR 不可能命中”

这是最容易被结果集合掩盖的区别：

- `prune(node)`：谓词已经对该节点 MBR 求值并返回 false，**有证据**地排除整支。
- 节点在 trace 中完全缺席（既无 push 也无 prune）：需求耗尽时遍历被挂起，该节点
  **只是还没走到**，不能断言它命中或不命中。

**结论 4：需求暂停时，延续状态保存在栈里；未到达的分支不是被裁剪。**
见 `testPausedTreeHasUnvisitedLeafNeitherPushedNorPruned`（两次 `request(1)` 后 `leafB`
在 trace 中完全缺席，且没有任何 `prune`；再次 request 才继续）。
栈本身是 `ImmutableStack`（[`ImmutableStack`](../main/java/com/github/davidmoten/rtree/internal/util/ImmutableStack.java)），
每次 push/pop 都是结构共享的新引用。

未命中 entry 不消耗需求：见 `testEntryOutsideBoundInVisitedLeafDoesNotConsumeDemand`
（a 命中耗尽唯一需求而暂停；下一次 request 中 b 在距离上界之外被跳过、不消耗需求，随后
leafB 被 MBR 距离裁剪并 complete）。

## 4. 距离上界：严格小于，先 MBR 距离后精确距离

`RTree.search(Point, double maxDistance)` / `search(Rectangle, double)` 使用
`g.distance(r) < maxDistance`：

- **严格小于**：距离恰好等于上界的条目不返回。距离语义由
  [`Geometry.distance(Rectangle)`](../main/java/com/github/davidmoten/rtree/geometry/Geometry.java)
  定义，点/圆实现见
  [`PointDouble`](../main/java/com/github/davidmoten/rtree/geometry/internal/PointDouble.java)、
  [`CircleDouble`](../main/java/com/github/davidmoten/rtree/geometry/internal/CircleDouble.java)。
- 先用 MBR 距离裁剪节点；任意几何类型再用 `search(g, maxDistance, distanceFn)` 做精确距离
  过滤（见 `RTree` 中对应重载）。

**结论 5：上界是严格的；MBR 距离足以排除的分支不会进入。**
见 `testMaxDistanceIsStrictAndPrunesByMbrDistance`（b 到原点恰为 √5，以 √5 为上界时被排除，
且 `prune(leafB)` 由 MBR 距离产生）。

## 5. circle：MBR 重叠后再做精确几何判定

圆搜索 `RTree.search(Circle)` 先以圆的 MBR（外接矩形）驱动树遍历，再用
[`Intersects.geometryIntersectsCircle`](../main/java/com/github/davidmoten/rtree/geometry/Intersects.java)
精确过滤 entry。于是一个叶子可能「MBR 与外接矩形重叠、但其中某些点不在圆内」。

**结论 6：圆查询中「MBR 层不可能」与「精确几何未命中」是两次不同判定。**
见 `testCirclePublicSearchFiltersExactGeometryAfterMbrTraversal`（点 a 位于 MBR 重叠叶子内但
在圆外，点 b 恰在圆边界上被命中，另一叶子由 MBR 排除）与
`testMbrOverlapVisitsLeafWhilePointMissIsNeitherHitNorPrune`（直接用外接矩形谓词验证：
a 未命中但既不是 hit 也不是 prune）。

## 6. nearest：排序、maxCount 与等距条目的承诺边界

`RTree.nearest(r, maxDistance, maxCount)` = 距离上界搜索 +
[`OperatorBoundedPriorityQueue`](../main/java/com/github/davidmoten/rtree/internal/operators/OperatorBoundedPriorityQueue.java)
（容量 `maxCount`，比较器
[`Comparators.ascendingDistance`](../main/java/com/github/davidmoten/rtree/internal/Comparators.java)）。
该 operator 在 `onStart` 向上游请求 `Long.MAX_VALUE`，因此底层树遍历走**快路径**，会访问
所有 MBR 距离小于 `maxDistance` 的分支；等距候选不能在树遍历阶段被裁剪。

**结论 7：nearest 结果按距离升序，数量受 `maxCount` 约束。**
见 `testNearestReturnsAscendingDistanceAndTraversesBothLeaves`（a,b,d,e 距离升序，两个叶子
都被访问）与 `testNearestMaxCountHonouredWithoutAssertingTieOrder`（只返回 2 个且距离不递减）。

**结论 8：等距（比较器返回 0）时不承诺全局顺序；只承诺数量不超过 `maxCount`、每个返回项
都是合法候选。**
见 `testNearestWithAllTiesDoesNotCommitToGlobalOrder`：四个到原点严格等距（5）的点，
`maxCount=2` 时断言恰好两个 onNext、二者不同且都来自等距集合，但**不断言** w/x/y/z 的先后。

## 7. cancel / 错误终止 / 重复 request 的确定行为

- **cancel 后不再产生 `onNext`**：两个引擎都在循环顶部检查 `isUnsubscribed()`
  （`Backpressure.searchAndReturnStack` 与 `OnSubscribeSearch.searchFast`），并通过
  `SearchObserver.fireCancel` 去重，cancel 事件至多一次。
  见 `testSlowPathCancelStopsTraversalAndReportsSingleCancel`、
  `testFastPathCancelAfterFirstHitSuppressesFurtherEmission`。
- **subscriber 回调或谓词抛错**：异常被 `SearchProducer.request` 捕获并转发到
  `subscriber.onError`，同时清空遍历栈；之后的 request 不会重启遍历。
  见 `testThrowingOnNextOnSlowPathRoutesToOnErrorAndStopsTraversal`、
  `testThrowingOnNextOnFastPathRoutesToOnError`、`testThrowingPredicateRoutesToOnError`。
- **背压为零**：`request(0)`/负值被记录但不遍历（结论 1）。
- **重复 request**：快路径已启动后再次 `Long.MAX_VALUE`（例如 `SafeSubscriber` 在
  `onCompleted` 后的补发）会被忽略，不重入、不重复 complete。
  见 `testRepeatedUnboundedRequestAfterCompletionDoesNotRestart`、
  `testRepeatedOneRequestsWalkFullTreeAndCompleteOnce`、
  `testSurplusDemandDoesNotReEnterTree`。

**完成的一个细微但确定的时序**（慢路径，直接继承自 RxJava `OnSubscribeFromIterable` 的
drain 循环）：循环先检查剩余需求、后判断栈是否为空。当**最后一个命中恰好耗尽最后一个需求
单位**时，栈被排空但 `onCompleted` 推迟到**下一次任意 demand 信号**才发出。
见 `testRepeatedOneRequestsWalkFullTreeAndCompleteOnce`、
`testFastAndSlowPathsReportIdenticalStructuralEvents` 中对第五次 request 的显式说明。
通过 `toBlocking()` 或连续 `request(1)` 的正常使用方式下，RxJava 会补发该信号，因此公开可观察
行为不变；这也是本文档不「修复」它的兼容性理由。

## 8. 订阅结束后遍历栈的引用生命周期

`SearchProducer.stack` 是延续状态：

- 暂停期间持有非空栈（恢复所必需）；
- 正常完成、cancel 或错误终止时置为 `null`，不再保留根以下的任何节点引用。
  producer 的 `node`（根）字段仍保留，因为快路径需要它；树本身是不可变的，这是必要且最小的
  保留。

见 `testStackHeldWhilePausedAndReleasedOnCompletion`、
`testStackReleasedOnCancelAndOnError`（通过反射读取 `SearchProducer.stack` 断言为 `null`）。

## 9. 结构共享如何影响节点身份

树是不可变的：[`RTree.add`](../main/java/com/github/davidmoten/rtree/RTree.java)
沿 selector 选定路径重建节点（[`NonLeafHelper.add`](../main/java/com/github/davidmoten/rtree/internal/NonLeafHelper.java)
中 `Util.replace` 只替换被改动的子节点），未被进入的分支**复用同一对象实例**。

**结论 9：一次 add 之后，未触及的兄弟子树对象身份不变；被进入的分支与根被重建。**
见 `testAdditionSharesUntouchedSiblingAndRebuildsDescendedBranch`（用 `IdentityHashMap`
按对象身份断言）与 `testQueryEventsCarrySharedNodeIdentityAcrossVersions`（同一兄弟叶子
在新旧两个版本的查询 `push` 事件中是 `==` 相同实例）。实践含义：observer 中以节点身份做
缓存/去重时，跨版本复用是安全且有意的；但不能用身份判断「这次查询是否访问过某区域」，因为
不同逻辑节点也可能因结构共享而身份相同。

**结论 10：空树没有根，因此不会安装 producer，也没有任何结构事件。**
见 `testEmptyTreeInstallsNoProducerAndEmitsNoStructuralEvents`。

## 10. 复杂度

- 一次查询访问的节点数由 MBR 裁剪决定。平均 `O(log n)`，最坏 `O(n)`
  （高度重叠的 MBR 会迫使多支都被访问）；这与 README 的搜索章节一致。
- `nearest` 的树遍历部分同上，另有容量 `maxCount` 的有界优先队列开销
  （每个候选 `O(log maxCount)`）；但等距/近距候选过多时仍可能访问线性数量的节点——
  上界裁剪是唯一的剪枝来源。
- observer 仅在 `enabled()` 时才计算栈深度（线性于当前栈深，即树高）；默认 noop observer
  不引入该成本。事件记录本身是测试支架，不在生产 API 中。

## 11. 兼容性与设计取舍

- `SearchObserver`、`RTree.search(condition, observer)`、`RTree.nearest(..., observer)`、
  `RTree.search(Point, double, observer)` 全部是**包级私有**，不进入公开 API；
  生产代码路径默认使用 `SearchObserver.noop()`，公开行为与已支持平台保持不变。
- 快路径观测递归只对内置的 `NonLeaf`/`Leaf`（`LeafDefault`、`NonLeafDefault`）发事件；
  其它 `Node` 实现（例如
  [`LeafFlatBuffers`](../main/java/com/github/davidmoten/rtree/fbs/LeafFlatBuffers.java)、
  [`NonLeafFlatBuffers`](../main/java/com/github/davidmoten/rtree/fbs/NonLeafFlatBuffers.java)）
  仍原样委托其 `searchWithoutBackpressure`，不改变其分配/递归特性。
- 未改变谓词被调用的次数与次序（每个节点在进入时恰好对自身 MBR 求值一次），未引入外部服务、
  新平台依赖或异步行为。
