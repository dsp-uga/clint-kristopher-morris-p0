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

Example of how to solve s1 from the terminal:

```
python s1_s3.py --outfile ./result/sp1.json
```
How to solve s2: 

```
python s1_s3.py --stopwords ./data/stopwords.txt --outfile ./result/sp2.json
```

s3:
```
python s1_s3.py --stopwords ./data/stopwords.txt --outfile ./result/sp3.json --punctuations True
```
s4:
```
python s4.py --outfile ./result/sp4.json
```
