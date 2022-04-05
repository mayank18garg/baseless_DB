JDKPATH = /Library/Java/JavaVirtualMachines/jdk-17.0.2.jdk/Contents/Home
LIBPATH = /Users/mayankgarg/Desktop/DBMSI/Assignemnt1/minibase_share/javaminibase/construction
CLASSPATH = $(JDKPATH)/lib/classes.zip:.:..:$(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

#test phase 2
BatchInsert:BatchInsert.java
	$(JAVAC) BatchInsert.java

batchinsert: BatchInsert
	$(JAVA) tests.BatchInsert test_data.txt 1 mayank_test

Query:Query.java
	$(JAVAC) Query.java

query: Query
	$(JAVA) tests.Query

Report:Report.java
	$(JAVAC) Report.java

report: Report
	$(JAVA) tests.Report mayank_test 1

# test buffer manager

BMTest:BMTest.java
	$(JAVAC) TestDriver.java BMTest.java

bmtest: BMTest
	$(JAVA) tests.BMTest

# test disk manager

DBTest:DBTest.java
	$(JAVAC) TestDriver.java DBTest.java

dbtest: DBTest
	$(JAVA) tests.DBTest

# test heapfile

HFTest:HFTest.java
	$(JAVAC) TestDriver.java HFTest.java

hftest: HFTest
	$(JAVA) tests.HFTest

# test btree

BTTest:BTTest.java
	$(JAVAC) TestDriver.java BTTest.java

bttest: BTTest
	$(JAVA) tests.BTTest

# test index

IndexTest:IndexTest.java
	$(JAVAC) TestDriver.java IndexTest.java

indextest: IndexTest
	$(JAVA) tests.IndexTest

# test join

JoinTest:JoinTest.java
	$(JAVAC) TestDriver.java JoinTest.java

jointest: JoinTest
	$(JAVA) tests.JoinTest

# test sort

SortTest:SortTest.java
	$(JAVAC) TestDriver.java SortTest.java

sorttest: SortTest
	$(JAVA) tests.SortTest

# test sortMerge

SMJoin:SM_JoinTest.java
	$(JAVAC) SM_JoinTest.java TestDriver.java

sortmerge: SMJoin
	$(JAVA) tests.SM_JoinTest


clean:
	\rm -f .class *~ \# core
