# Introduction

## Lighting Preview

### Infrastructure 

storage, communication, computation

目标：看上去非分布式的系统，抽象的，容错性高的，简单的接口

抽象的过程(Abstraction)，这是难以实现的



### Tool

RPC：掩盖在不可靠网络上通信的事实

Thread：结构化的并发方式，一种并发操作的视角

Locks：



### Performance

Scalability：理想情况下，两台计算机就会有两倍算力、性能或吞吐量，这对设计有着很高的要求。这种可扩展性很难是无限的，因为会有瓶颈（浏览器、数据库）。架构设计。



### Fault Tolerance

大型分布式系统的常见问题是，集群中很可能会有卡顿、崩溃和执行错误等，甚至是网络的中断。扩容把错误变成了常见的问题。

Availability：一直可用，比如进行备份，建立在特殊的错误类型上

Rocoverability：

Non-volatile storage：日志、检查点等

Replication：副本、备份，为了有更好的容错效率，人们常常保证副本之间有不同的故障概率，也会把副本分开进行存储，但这也带来了通信的代价



### Consistency

通常有多个副本。在非分布式系统中，存贮一般是没有歧义的，但是在分布式系统中，存储带来了许多的副本（版本），这意味着对于存储的更新需要同步状态。

强一致性、弱一致性：前者有着更高的代价，人们也会关注后者



## MapReduce

### 意义

由谷歌设计和使用，用于TB级数据上的计算

框架，让工程师专注于自己的应用程序，不用考虑在数千台计算机上分发消息，避免错误等问题

只要完成Map和Reduce两个模块，就可以完成



### 原理

[MapReduce](../articles/mapreduce.pdf)

