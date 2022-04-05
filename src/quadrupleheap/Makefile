JDKPATH = /Library/Java/JavaVirtualMachines/jdk-17.0.2.jdk/Contents/Home
LIBPATH = /Users/mayankgarg/Desktop/DBMSI/Assignemnt1/minibase_share/javaminibase/construction
CLASSPATH = $(JDKPATH)/lib/classes.zip:.:..:$(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

PROGS = together

all: $(PROGS)

quadruple:Quadruple.java
	$(JAVAC) Quadruple.java

datapageinfo:DataPageInfo.java
	$(JAVAC) DataPageInfo.java

quadruplehf:QuadrupleHeapfile.java
	$(JAVAC) QuadrupleHeapfile.java

thfpage:THFPage.java
	$(JAVAC) THFPage.java

tscan:TScan.java
	$(JAVAC) TScan.java

together:*.java
	$(JAVAC) *.java

clean:
	\rm -f .class *~ \# core