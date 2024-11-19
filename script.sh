### MapReduce Review Analyzer - Top 150terms per category ###


#removing the output from previous runs
#replace *user with your user
echo "**************************removing previous output**************************"
hadoop fs -rm -r -f  hdfs:///user/*user/RevsPerCategory_Counter
hadoop fs -rm -r -f  hdfs:///user/*user/TermsFiltered
hadoop fs -rm -r -f  hdfs:///user/*user/PreparedPerTerm
hadoop fs -rm -r -f  hdfs:///user/*user/ChiSquaredValues
hadoop fs -rm -r -f  hdfs:///user/*user/Top150terms
hadoop fs -rm -r -f  hdfs:///user/*user/Top150termsSorted


#change to the correct directory (classes, not main with java code, otherwise cannot create jar)
cd $(pwd)/target/classes

#create the jar file (I used intellij locally, so jar is created from the generated classes)
echo "**************************generating the jar file**************************"
jar -cf ../generated.jar Main.class ProcessInput TermFiltering PrepPerTerm ChiSquaredValuesCalc Top150terms AlphabeticalSort


#change directory to where the jar file is stored
cd ..

#run all the jobs
echo "**************************running the jobs**************************"
hadoop jar generated.jar Main

echo "**************************collecting output**************************"
#collect output
#replace *user with your user
hadoop fs -getmerge hdfs:///user/*user/Top150termsSorted output.txt
