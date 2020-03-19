# simplekv
一个简单的基于sstable的kvstore.

# TODOS
1. 多线程Compact (目前只有一个)
2. 如何更高效的使用内存表? 目前达到一定大小后导出文件, 但是WAL可能会很大.
3. 将index文件内嵌到datafile中
4. 更好的Compact策略? 
