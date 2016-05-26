# !/bin/sh  
nohup /home/pubsrv/spark-1.6.1/bin/spark-submit --master spark://spark0:7077 --name suxin-test1 --class jianglong_task --total-executor-cores 4 --executor-cores 1 --driver-memory 2g --executor-memory 2g  --jars /home/gaoyuan/jars/aws-java-sdk-1.10.61.jar,/home/gaoyuan/jars/hadoop-aws-2.7.1.jar,/home/gaoyuan/jars/guava-11.0.2.jar,/home/gaoyuan/jars/mysql-connector-java-5.1.9.jar  /home/suxin/compare_nation/target/scala-2.10/test-assembly-1.0.jar  AKIAJIVCMRHJ56IM7IVA V0W5+gCYJ4Mvmi5Wfp4KN/uUxZ48KvAfTguPxY3Z &

/bin/mail -s "Daily Report with living" -a /home/suxin/compare_nation/mail_jianglong/*.csv s1264242822@qq.com < /home/suxin/mail_box &&
rm /home/suxin/compare_nation/mail_jianglong/*.csv &&