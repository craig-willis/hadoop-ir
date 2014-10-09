#!/bin/bash
JAR=../mirex-0.3.jar

#
# Prepare
#
hadoop fs -mkdir warc-files
hadoop fs -put test.warc.gz ./warc-files/
hadoop fs -put wt2010-topics.queries-only ./
hadoop jar $JAR nl.utwente.mirex.AnchorExtract warc-files/* anchors
#
# Key-value files
#
hadoop jar $JAR nl.utwente.mirex.TrecRun          KEYVAL anchors/* TrecOut wt2010-topics.queries-only
hadoop fs -cat ./TrecOut/part*
hadoop jar $JAR nl.utwente.mirex.QueryTermCount   KEYVAL anchors/*  wt2010-topics.queries-only  wt2010-topics.stats
hadoop fs -cat ./wt2010-topics.stats
hadoop jar $JAR nl.utwente.mirex.TrecRunBaselines KEYVAL anchors/* BaselineOut wt2010-topics.stats
hadoop fs -cat ./BaselineOut/part*
#
# Warc files
#
hadoop jar $JAR nl.utwente.mirex.TrecRun          WARC warc-files/* TrecOut2 wt2010-topics.queries-only
hadoop fs -cat ./TrecOut2/part*
hadoop jar $JAR nl.utwente.mirex.QueryTermCount   WARC warc-files/* wt2010-topics.queries-only  wt2010-topics.stats2
hadoop fs -cat ./wt2010-topics.stats2
hadoop jar $JAR nl.utwente.mirex.TrecRunBaselines WARC warc-files/* BaselineOut2 wt2010-topics.stats2
hadoop fs -cat ./BaselineOut2/part*

# To remove:
#hadoop fs -rmr BaselineOut* MIREX-tmp TrecOut* anchors warc-files wt2010-topics.queries-only wt2010-topics.stats*

