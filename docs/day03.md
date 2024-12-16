回顾上一天的面试题，搞定pom的依赖冲突，费事较长，比较了很多解决方案，到下午才解决，不了解谁和谁的依赖起冲突，跟着很多文档走去尝试解决报错，尝试更换mysql，但是太麻烦了，有同事通过更换aliyun maven解决依赖冲突问题，

更换maven ：1 更换idea里面设置maven的本地位置，

2，在新maven文件中conf.settings.xml 里面找到:

```
<localRepository>E:\apache-maven-3.8.3\maven\repository</localRepository>
```

这里是repository的指定位置。没有这个我的新依赖在idea中生成的东西他会报错，

3.更改本机的环境变量，对MavenHOME的指定路径进行更换

4，在idea中，配置pom，让系统自己生成依赖，



全部更换完成，测试FlinkCDC读取mysql依旧报环境依赖问题，



继续进行查找，在博客中对关于依赖关系的了解收获更多

同事说：不要怕错，解错就是进步的关键

原本对报错的困扰我心烦意乱，同事使让我重振精神

在解决不了依赖冲突，转行对插件进行安装：



配置Redis，Hbase，

redis中有新的错误出现：

1，对yum的报错，解决方案是：

yum -y install centos-release-scl

更新yum

网站地址：https://blog.csdn.net/qq_37356038/article/details/144220952

2.gcc版本过低报错

make时，出现很多错误

解决方案：

scl enable devtoolset-9 bash

yum install devtoolset-9

yum install --nogpgcheck devtoolset-9

scl enable devtoolset-9 bash

对gcc进行版本升级，

我们之前写离线数仓时的版本时4.8.5,更新止最近9.0多

最后解决hbase的安装报错

1.在配置单节点时无报错，但在集群启动HBASe时报错java的环境问题

报错是：

![image-20241213093927454](C:\Users\264620\AppData\Roaming\Typora\typora-user-images\image-20241213093927454.png)

以上的信息可以得到是环境问题，目前正在解决；