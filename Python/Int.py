# Databricks notebook source
# MAGIC %md 
# MAGIC ###count occurrence of each character

# COMMAND ----------

str = 'Learn Python Challenge'
str = str.lower()
dict ={}
for char in str:
  if char in dict:
    dict[char]+=1
  else:
    dict[char]=1

for key in dict:
  print(key,dict[key])


# COMMAND ----------

#which char occured max number of times
#this approach will return only one output. what if you have more than one char having same no. of occurrences.
char=''
n=0
for k,v in dict.items():
  if v>n:
    char=k
    n=v
char

# COMMAND ----------

char=''
n=0
l = []
for k,v in dict.items():
  if v>=n:
    l.append(k)
    n=v
print(l,'No. of occurrences:',n)

# COMMAND ----------

# MAGIC %md
# MAGIC ###find index of a word

# COMMAND ----------

def ind_fuc(string,word):
  try:
    word=word.lower()
    return string.index(word)
  except Exception as e:
    return 'Word does not exists!'
print(ind_fuc(str,'Python'))

# COMMAND ----------

#without in-built functions
def ind_fuc(string,word):
  try:
    word=word.lower()
    l = len(word)
    for i in range(0,len(string)-len(word)+1):
      #print(string[i:i+len(word)],word)
      if string[i:i+len(word)]==word:
        return i
      elif len(string)==i+len(word):
        raise Exception
      
  except Exception as e:
    return 'Word does not exists!'
print(ind_fuc(str,'ge'))

# COMMAND ----------

# MAGIC %md
# MAGIC ###sort and merge two arrays

# COMMAND ----------

l1 = [10,3,8,1,4]
l2 = [9,2,6,7,5,0]
#sorting both arrays using bubble-sort
for i in range(len(l1)-1):
  for j in range(len(l1)-1):
    if l1[j]>l1[j+1]:
      l1[j],l1[j+1]=l1[j+1],l1[j]

for i in range(len(l2)-1):
  for j in range(len(l2)-1):
    if l2[j]>l2[j+1]:
      l2[j],l2[j+1]=l2[j+1],l2[j]
      
print(l1,l2)

# COMMAND ----------

#merging both sorted array
s_array=[]
size1 = len(l1)-1
size2 = len(l2)-1
#print(size1,size2)
while (size1>=0 and size2>=0):

  if l1[size1]>l2[size2]:
    s_array=[l1[size1]]+s_array
    size1-=1
    #size1 = len(l1[:size1-1])
  elif l1[size1]<l2[size2]:
    s_array=[l2[size2]]+s_array
    size2-=1
    #size2 = len(l2[:size2-1])
  if size2==-1:
    s_array=[l1[0]]+s_array
  elif size1==-1:
    s_array=[l2[0]]+s_array
  #print(size1,size2)
  #print(s_array)
  
    
s_array

# COMMAND ----------

#Selection Sort
l = [3,5,7,23,1,5,4,2,1,8,4,0]

for i in range(len(l)):
  min_inx = i
  for j in range(i+1,len(l)):
    if l[j]<l[min_inx]:
      min_inx = j
  l[i],l[min_inx]=l[min_inx],l[i]

l

# COMMAND ----------

# MAGIC %md
# MAGIC ###Similar letters in 2 strings

# COMMAND ----------

a = 'abcd'
b = 'bdca'
lst1 = []
lst2 = []
for i in a:
  ord(i)-97)


# COMMAND ----------

l=[6]
l = l+[2]
l

# COMMAND ----------

len([])

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


