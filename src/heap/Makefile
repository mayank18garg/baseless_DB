JDKPATH = /Library/Java/JavaVirtualMachines/jdk-17.0.2.jdk/Contents/Home
LIBPATH = /Users/mayankgarg/Desktop/DBMSI/Assignemnt1/minibase_share/javaminibase/construction
CLASSPATH = $(JDKPATH)/lib/classes.zip:.:..:$(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

PROGS = together

all: $(PROGS)

hf:Heapfile.java
	$(JAVAC) Heapfile.java

hp:HFPage.java
	$(JAVAC) HFPage.java

tuple:Tuple.java
	$(JAVAC) Tuple.java

scan:Scan.java
	$(JAVAC) Scan.java

together:*.java
	$(JAVAC) *.java

clean:
	\rm -f *.class *~ \#* core 
