# Hadoop错误总结
问题1、 failed on connection exception: java.net.ConnectException: Connection refused; For more details see:  
http://wiki.apache.org/hadoop/ConnectionRefused**

在window下调试mapReduce发生如上异常
1.1、确认core-site.xml，yarn-site.xml的hostname是不是正确，
一定不能包含"-"字符。     
1.2、确认hdfs-site.xml
    <property>
        <name>dfs.permissions</name>
        <value>false</value>
    </property>	    
1.3、确认cat /etc/sysconfig/network
cat /etc/hosts 是不是和hostname的一致。
