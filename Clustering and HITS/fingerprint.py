import re
from pyspark import SparkContext
import sys
import unicodedata
import string


def transform_to_unicode(text):
    return unicodedata.normalize('NFKD',text).encode('ascii','ignore').decode()

def text_preprocess(x):
    restaurant = x.strip().lower()
    restaurant = re.sub('[^a-z0-9\s]+','',restaurant)
    return restaurant

def get_frequency(list):
    freq_dict = {}
    for i in list:
        if i in freq_dict:
            freq_dict[i] += 1
        else:
            freq_dict[i] = 1
    return sorted(freq_dict.items(), key=lambda x: x[1], reverse=True)

def formatToString(list):
    val = ""
    for word in list[:-1]:
        val+=word[0] + "(" + str(word[1])+ "),"
    val += list[-1][0]+"("+str(list[-1][1])+")"
    return val
if __name__ == "__main__":
    #if len(sys.argv) != 2:
       # print("Usage: hits <file> <iterations>", file=sys.stderr)
       # exit(-1)
    sc = SparkContext(appName="fingerprint_clustering")
    data = sc.textFile("test_file.txt")
    #print(len(data.collect()))
    data = data.map(lambda x: (" ".join(sorted(list(set(transform_to_unicode((text_preprocess(x))).split())))),x))#.map(lambda text: transform_to_unicode(text))

    #data = data.map(lambda x: x.split()).map(lambda x: sorted(list(set(x))))
    data = data.groupByKey().mapValues(list).mapValues(get_frequency).filter(lambda x: len(x[1])>=2)
    output = data.collect()


    output.sort(key=lambda x:len(x[1]),reverse=True)
    print(len(output))

    fhandle = open("p1.txt","w")
    for word in output[:-1]:
        fhandle.write(word[0]+":"+formatToString(word[1])+"\n")
    fhandle.write(str(output[-1][0])+":"+formatToString(output[-1][1]))


    #rdd = lines.map(lambda x:(" ".join(sorted(list(set(translate(re.sub('[^a-z0-9\s]+','',x.strip().lower())).split())))),x))




