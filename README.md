# simplekv
一个简单的基于sstable的kvstore. 基本想法: <br>
 有一个MemTable来存储当前的写入,  到达一定阈值后将MemTable转为SSTable文件并构建索引文件. <br>
 系统维护一个稀疏索引的内存对象来快速定位某个key所在的文件. <br>

# 怎么使用?
参考[Test.java](./src/test/java/Test.java) 

# TODOS
1. 多线程Compact (目前只有一个)
2. 如何更高效的使用内存表? 目前达到一定大小后导出文件, 但是WAL可能会很大.
3. 将index文件内嵌到datafile中
4. 更好的Compact策略? 参考下LevelDB/RocksDB
