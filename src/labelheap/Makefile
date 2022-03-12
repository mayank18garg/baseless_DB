JDKPATH = /Users/jagrutidhondage/Library/Java/JavaVirtualMachines/openjdk-17.0.1/Contents/Home
LIBPATH = /Users/jagrutidhondage/Documents/DBMSI/minjava/javaminibase/construction
CLASSPATH = $(JDKPATH)/lib/classes.zip:.:..:$(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

PROGS = together

all: $(PROGS)

label:Label.java
	$(JAVAC) Label.java

labelhf:LabelHeapfile.java
	$(JAVAC) LabelHeapfile.java

lhfpage:LHFPage.java
	$(JAVAC) LHFPage.java

together:*.java
	$(JAVAC) *.java

clean:
	\rm -f *.class *~ \#* core 
