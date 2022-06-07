





先简单介绍下 WAL 的格式， WAL 会用最大不超过 32 KiB 的多个 Block 进行保存，
目前 Pebble 默认使用的是 recyclable 格式的 Block(即 WAL 文件在不需要时可以被回收复用，以减少 sync 文件元信息)


复用 wal 文件信息，优化 wal  文件的空间分配，减少 page cache 中文件元信息的更新开销。

在大压力写下，会出现多个 .log 文件同时存在的现象，想要看看为什么会有这个现象。

我们先看看 Rocksdb 的 wal 创建以及清理过程，其中 recycle_log_file_num 是如何 reuse log 的。

在 CreateWal 文件的时候，Rocksdb 会为这个 wal 创建一个 PosixEnv 下的文件句柄，以及文件名，
并创建一个文件 writer ，用来后续的数据写入。


如果我们能够 reuse log 文件名，则在通过 open 系统调用打开已存在文件的时候，
不需要创建新的 dentry 和 inode ，且不需要将这一些元数据更新到各自 dcache/inode-cache 中的相应 hash 表中，
所以重用文件名这里的优化就体现在内核对文件的一些操作逻辑上。




大并发写rocksdb 会发现部分log文件可能存在的时间较长，且同时存在多个log 数目。

对于第一个问题 log存在的时间较长，即是由recycle_log_file_num 参数控制，它会不断得复用一些过期(不接受写入)的log，并且这一些log不会被回收。这个参数能够提升log文件的复用，减少对文件元数据的操作，加速SwitchMemtable的过程。

对于第二个问题 log存在多个，则是由于max_write_buffer_number参数的问题，它允许同时存在多个memtable，如果写入量较大，则imm 排队flush，则这个过程中的imm 对应的log文件是不会清理的，而recycle_log_file_num 则会回收一些log_num，且让这一些log不会被清理，所以会同时出先多个log_num。

需要注意的是recycle_log_file_num 这个参数回收的log不会被清理。

