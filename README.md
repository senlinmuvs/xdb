# XDB
# 像操作关系数据库一样操作kv数据库
批量查改删数据工具。支持连key（类似连表），条件查询，字段选择。
目前较粗糙但足以满足很多场景使用。目前仅支持ssdb。
```
sen@x:/mnt/c/code/go/xdb$ ./xdb -x "find h:user:%d{id(0,100000011)}(id,ut)"
id      ut
100000001       1610539080041
100000002       1610539080041
100000003       1610539080041
100000004       1610539080041
100000005       1610539080041
100000006       1610539080041
100000007       1610539080041
100000008       1610539080041
100000009       1610539080041
100000010       1610539080041
xdb count: 10
```

# 常用操作
## find
```
#查找所有以h:pk开头的hash key，条件：hash条目cont=x并且img为空串
find h:pk:%d{cont=x,img=}(*)

#查找所有以h:pk:开头的hash key，条件：key上点位符%s处的值满足正则/[^\d]+/
find h:pk:%s{%0/[^\d]+/}
```
## cp
### cp 复制的源key  复制的目标key
```
先查找以h:user:开头的hash，把查找到的每条hash的hrtBid值填入后面的z:bk:@hrtBid:st:%d:pks再查找并把找到的zset数据复制到目标key中。
目标key中%0是引用源key上第0个点位符的值，所以这里是每个h:user:%d的%d对应值，即uid。
cp h:user:%d|z:bk:@hrtBid:st:%d:pks z:user:%0:hrtpks:by:lst
```
## set
### set 需要查找的key   需要set的key
find后修改kv
```
set h:user:%d zyh:@zyh:uid,%0
```
find后修改zset
```
#先查找h:user:%d，把每个hash的第0个点位符的值，即uid填入目标key的%0上，最后把找到的目标zset用后面的数据（即100000010,1608707314083）更新
set h:user:%d z:user:%0:follows:by:lst,100000010,1608707314083

set h:user:%d{jut[1610539080041,)} z:user:by:lst,%0,1610539080041
```
## hset
### hset 需要查找的key  需要hset的key
find后修改hash
```
#查找模板h:work:%d，然后把hash字段img用内置函数UnQuote()处理去除引号
hset h:work:%d img=UnQuote()

#查找模板h:pk:%d，再把hash字段st用内置函数DelField()处理，即删除st字段
hset h:pk:%d st=DelField()

#查找模板h:user:%d，条件为ut<1的，ut字段赋值为ct字段的值
hset h:user:%d ut(,1)=@ct
```
## del
```
#删除所有以h:pk:开头且%s位置匹配/[^\d]+/正则的hash数据
del h:pk:%s{%0/[^\d]+/}

#删除所有以h:bk:开头的，并且条件为hash字段stat值在区间(,1)，即小于1
del h:bk:%d{stat(,1)}

#删除所有key匹配此模板的kv
del tag:%s:id

#删除所有key匹配此模板的zset
del z:work:%d:tagIds

#删除所有key匹配模板z:user:%d:pks，且条件为：
以zset的key值连接另一个key(h:pk:@key)后，且h:pk:@key的id字段=0
del z:user:%d:pks{h:pk:@key(@id=0)}
```
## import
user.csv数据导入h:user:%0，%0引用user.csv第一列数据
```
import file:///mnt/c/user.csv h:user:%0
```

## export
把查找到的所有h:pk:开头的hash的全部字段导出到u.csv
(*)表示全部字段,(id,name)则表示id与name字段
```
find h:pk:%d(*) > u.csv
```

# 符号
## ()
条件值区间，字段选择

## {}
条件

## @
引用前导hash字段

## |
连key操作

## %
key中值占位符
* %d表示数字
* %s表示字符串
* %0,%1等表示引用第0个或第1个占位符上的值

# key风格
* h:开头表示hash
* z:开头表示zset
* 无此前缀表示kv

其它风格程序里有常量可设置