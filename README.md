# clint-kristopher-morris-p0

Introduction:
-----------------
This project featured four sub-projects, testing various approaches of word counting and calculating TF-IDF scores.

* S1 takes the books contained within the data folder and calculates the top 40 most commonly used words after converting to uniform letter case.

* S2 repeats the task of S1 but also removes “stop words” located in the data folder.

* S3 assumes the goals of S2 in addition it removes leading and trailing punctuation.

* S3 calculates the top 5 TF-IDF scores from each book and complies them together.

Technologies Used:
-----------------
- Python 3.5
- Apache Spark

How to Implement The Models
------------------

This project was segmented into two main files:

s1-s3.py obviously pertaining to sections s1, s2 and s3. While, s4.py handle the requirements from s4.

Examples of how to solve problems s1-s4  from the terminal are listed below:

```
python s1_s3.py --outfile ./result/sp1.json

python s1_s3.py --stopwords ./data/stopwords.txt --outfile ./result/sp2.json

python s1_s3.py --stopwords ./data/stopwords.txt --outfile ./result/sp3.json --punctuations True

python s4.py --outfile ./result/sp4.json
```

The files are dynamic enough to allow you to specify the following information:

*  --file which can be a single text document to a directory.
* --stopwords location of the text files with the stop words.
* --top how many of the sorted counted words should be returned.
* --punctuations if True leading and trailing  punctuation will be removed.
* --outfile location of where the resultant json file is stored

Authors
--------------

  [Clint Morris](https://github.com/clint-kristopher-morris)


