# 点赞功能核心实现
实现了点赞、取消点赞以及店点赞查看功能
## 版本信息
- JDK8
- Springboot 2.x
## 依赖中间件
### rocketmq 4.8.0
broker配置如下:
```properties
brokerClusterName = DefaultCluster
brokerName = broker-a
brokerId = 0
deleteWhen = 04
fileReservedTime = 48
brokerRole = ASYNC_MASTER
flushDiskType = ASYNC_FLUSH
# CommitLog 文件大小
mappedFileSizeCommitLog=2097152
# ConsumerQueue 文件大小
mappedFileSizeConsumeQueue=3000
#storePathRootDir=/tmp/store
#storePathCommitLog=/tmp/store/commitlog
# 最大磁盘使用率
diskMaxUsedSpaceRatio=98
# 事务消息超时时间
transactionTimeout=60000
# 事务消息最大重试时间
transactionCheckMax=6
# 事务消息重试时间间隔
transactionCheckInterval=5000
```
### MYSQL
当前用的是5.0，8版本的应该也可以
### Redis
可以用单机，也可以用集群

