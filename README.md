# BigData_WikiAnalysis
Aggregated page view statistics for Wikimedia projects is available at
http://dumps.wikimedia.org/other/pagecounts-raw/. This page gives access to files that contain the
total hourly page views for Wikimedia project pages by page. Information on the file format is given
on this page view statistics page.

Required Tasks for the application

1. Use HDFS and MapReduce to identify the popularity of Wikipedia projects by the number of
pages of each Wikipedia site which were accessed over an x hour period. Your job should allow
you to directly identify from the output the most popular Wikipedia sites accessed over the time
period selected. You can choose whichever x hour period you wish from the files available on
the page view statistics page, with the constraint that 3<=x<=6.
2. Use HDFS and MapReduce to identify the average page count per language over the same
period, ordered by page count.
