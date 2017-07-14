add jar target/hive-dq-1.0-SNAPSHOT.jar ;
create temporary function bitdist as 'org.notmysock.hive.udf.BitDistribution';
create temporary function bitcountdist as 'org.notmysock.hive.udf.BitCountDistribution';
create temporary function bytedist as 'org.notmysock.hive.udf.ByteDistribution';
create temporary function bitchance as 'org.notmysock.hive.udf.BitProbabilityDistribution';
create temporary function byteuniq as 'org.notmysock.hive.udf.ByteUniqueness';
