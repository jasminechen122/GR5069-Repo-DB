# Databricks notebook source
def word_count(text):
    counts = dict()
    if text == "":
        return counts
    else:
        x = text.split(" ")
        for e in x: #for each element in the text list x
            if e in counts: #count the freqeuncy of the element
                counts[e]+=1 
            else:
                counts[e] = 1
        return counts

# COMMAND ----------

#test your function with the following sample data

#Should return {'It': 1, 'best': 1, 'it': 1, 'of': 2, 'the': 2, 'times': 2, 'was': 2, 'worst': 1}
text1 = "It was the best of times it was the worst of times"
print(word_count(text1))

#Should return {}
text1 = ""
print(word_count(text1))
