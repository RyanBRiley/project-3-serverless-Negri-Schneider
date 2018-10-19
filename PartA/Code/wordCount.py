import operator
import pyspark
import re
import sys

stop_words = ['a', 'about', 'above', 'after', 'again', 'against', 'all', 'am', 'an', 'and', 'any', 'are', 'arent', 'as', 'at', 'be', 'because', 'been', 'before', 'being', 'below', 'between', 'both', 'but', 'by', 'cant', 'cannot', 'could', 'couldnt', 'did', 'didnt', 'do', 'does', 'doesnt', 'doing', 'dont', 'down', 'during', 'each', 'few', 'for', 'from', 'further', 'had', 'hadnt', 'has', 'hasnt', 'have', 'havent', 'having', 'he', 'hed', 'hell', 'hes', 'her', 'here', 'heres', 'hers', 'herself', 'him', 'himself', 'his', 'how', 'hows', 'i', 'id', 'ill', 'im', 'ive', 'if', 'in', 'into', 'is', 'isnt', 'it', 'its', 'its', 'itself', 'lets', 'me', 'more', 'most', 'mustnt', 'my', 'myself', 'no', 'nor', 'not', 'of', 'off', 'on', 'once', 'only', 'or', 'other', 'ought', 'our', 'ours   ourselves', 'out', 'over', 'own', 'same', 'shant', 'she', 'shed', 'shell', 'shes', 'should', 'shouldnt', 'so', 'some', 'such', 'than', 'that', 'thats', 'the', 'their', 'theirs', 'them', 'themselves', 'then', 'there', 'theres', 'these', 'they', 'theyd', 'theyll', 'theyre', 'theyve', 'this', 'those', 'through', 'to', 'too', 'under', 'until', 'up', 'very', 'was', 'wasnt', 'we', 'wed', 'well', 'were', 'weve', 'were', 'werent', 'what', 'whats', 'when', 'whens', 'where', 'wheres', 'which', 'while', 'who', 'whos', 'whom', 'why', 'whys', 'with', 'wont', 'would', 'wouldnt', 'you', 'youd', 'youll', 'youre', 'youve', 'your', 'yours', 'yourself', 'yourselves', '']

def word_cleanup(word):
    clean = re.sub(r'[^A-Za-z]+', '', word).lower()
    return (clean, 1) if clean not in stop_words else ('', 0)

def main():
    #Intialize a spark context
    with pyspark.SparkContext('local', 'PySparkWordCount') as sc:
        filename = sys.argv[1]
        #Get a RDD containing lines from this script file  
        lines = sc.textFile(filename)
        #Split each line into words and assign a frequency of 1 to each word
        words = lines.flatMap(lambda line: line.split()).map(word_cleanup)
        #count the frequency for words
        counts = words.reduceByKey(operator.add)
        #Sort the counts in descending order based on the word frequency
        sorted_counts =  counts.sortBy(lambda x: x[1], False)
        #Get an iterator over the counts to print a word and its frequency
        output_file = open('output.txt', 'w')
        i = 0
        for word, count in sorted_counts.toLocalIterator():
            i += 1
            if i > 2000:
                return
            output_file.write('{}\t{}\n'.format(word, count))

if __name__ == '__main__':
    main()
