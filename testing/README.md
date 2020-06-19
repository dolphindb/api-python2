### DolphinDB 单节点部署
DolphinDB 单节点安装非常容易。只要DolphinDB软件包下载、解压缩后即可运行
例如解压到如下目录

```
/DolphinDB_LINUX_V0.5
```

### 单机模式

#### 1. 软件授权许可更新

如果用户拿到了企业版试用授权许可，只需要把如下文件替换成新的授权许可即可

```
/DolphinDB_LINUX_V0.5/server/dolphindb.lic
```

#### 2. DolphinDB Server运行

```
// 1. 进入server目录
./DolphinDB_LINUX_V0.5/server/

// 2. 直接运行dolphindb

// Linux: execuate the following command
./dolphindb


//windows: execaute the following command
dolphindb.exe
```

系统默认端口号是8848. 如果需要指定其它端口可以通过如下命令行

```
//linux
./dolphindb -localHost:8900:local8900

//windoows
dolphindb.exe -localHost:8900:local8900
```

DolphinDB 默认使用软件授权书上的最大内存数，用户也可以根据实际情况来做出调整。DolphinDB内存使用是按照GB来算
的，这个设置在启动dophindb 的时候可以通过参数 -maxMem 来实现

```
//linux
./dolphindb -localHost:8900:local8900 -maxMem 32

//windoows
dolphindb.exe -localHost:8900:local8900 -maxMem 32
```

#### 3.连接到DolphinDB Server

