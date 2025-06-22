# Databricks notebook source
lst = [1,'manish',1]
tp = (1,'manish',1)
st = set([1,'manish',1])
dict = {'a':'manish',2:'Somya'}

# COMMAND ----------

type(st)

# COMMAND ----------

for k,v in dict.items():
  print(k,v)

# COMMAND ----------

dict.update({'a':'Bubul'})
dict

# COMMAND ----------

for k,v in enumerate(dict.items()):
  print(k,v[0],v[1])

# COMMAND ----------

for i,j in zip(lst,lst):
  print(i,j)

# COMMAND ----------

#enumerate and zip
for i in lst:
  if 'm' in str(i):
    print(lst.index(i))

# COMMAND ----------

for i in range(1,6):
  if i==2:
    continue
  print(i)

# COMMAND ----------

'''import os
modular programming
file handling concept
multi threading concept
exception handling
oops

apache kafka
SQL
polybase
mpp architecture

Azure Devops
build
artifect
branches
'''

# COMMAND ----------

def canConstruct(ransomNote, magazine):
        """
        :type ransomNote: str
        :type magazine: str
        :rtype: bool
        """
        for i,l in enumerate(ransomNote):
            if l not in magazine:
                return False
            magazine=magazine.replace(l,'',1)
        return True
            

canConstruct('aa','aab')    

# COMMAND ----------

s = 'manisha'
s.replace('a','',1)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Reverse an array

# COMMAND ----------

#using slice
st = 'Manish Kaushik'
print(st[::-1])

# COMMAND ----------

#using list
st_lst = list(st)
st_lst.reverse()
print(''.join(st_lst))

# COMMAND ----------

#
def rev(ar,start,end):
  assert(start<=end)
  ar = list(ar)
  #print(ar)
  while start<end:
    ar[start],ar[end]=ar[end],ar[start]
    start+=1
    end-=1
  return ''.join(ar)
  
try:
  st = 'Manish Kaushik'
  print(rev(st,2,4))
  
  
except:
  print('Error')

# COMMAND ----------

# Recursive python program to reverse an array

# Function to reverse A[] from start to end
def reverseList(A, start, end):
	if start >= end:
		return
	A[start], A[end] = A[end], A[start]
	reverseList(A, start+1, end-1)

# Driver function to test above function
A = [1, 2, 3, 4, 5, 6]
print(A)
reverseList(A, 0, 5)
print("Reversed list is")
print(A)
# This program is contributed by Pratik Chhajer


# COMMAND ----------

# MAGIC %md
# MAGIC ###Rotation of Array

# COMMAND ----------

#Left rotation
def rot(arr,no):
  last_ele = arr[-no:]
  
  n=len(arr)-no
  while n>0:
    arr[n+no-1]=arr[n-1]
    n-=1
  last_ele.extend(arr[no:])
  return last_ele
   

a = [1,2,3,4,5,6]
print(rot(a,2))

# COMMAND ----------

#Using temp list
def rot1(arr,no):
  temp_lst = arr[no:]
  temp_lst.extend(arr[:no])
  return temp_lst
  
a = [6,7,8,9,10,11,12,13]
print(rot1(a,2))

# COMMAND ----------

#Rotate one by one
def rot2(arr,d):
  l = len(arr)
  for i in range(0,d):
    p = arr.pop(0)
    arr.append(p)
  return arr

a = [4,5,6,7,8,9]
print(rot2(a,3))    

# COMMAND ----------

#Rotae one by one without list func
def rot3(arr,d):
  l = len(arr)
  for i in range(d):
    p = arr[0]
    for j in range(n-1):
      arr[j]=arr[j+1]
      #arr[1:].append(p)
    arr[-1]=p
  return arr

a = [4,5,6,7,8,9]
print(rot2(a,2))    

# COMMAND ----------

# Python3 program to rotate an array by
# d elements
# Function to left rotate arr[] of size n by d


def leftRotate(arr, d, n):
	d = d % n
	g_c_d = gcd(d, n)
	for i in range(g_c_d):

		# move i-th values of blocks
		temp = arr[i]
		j = i
		while 1:
			k = j + d
			if k >= n:
				k = k - n
			if k == i:
				break
			arr[j] = arr[k]
			j = k
		arr[j] = temp

# UTILITY FUNCTIONS
# function to print an array


def printArray(arr, size):
	for i in range(size):
		print("% d" % arr[i], end=" ")

# Function to get gcd of a and b


def gcd(a, b):
	if b == 0:
		return a
	else:
		return gcd(b, a % b)


# Driver program to test above functions
arr = [1, 2, 3, 4, 5, 6, 7]
n = len(arr)
d = 2
leftRotate(arr, d, n)
printArray(arr, n)

# This code is contributed by Shreyanshi Arun


# COMMAND ----------

# MAGIC %md
# MAGIC ###Sort an Array

# COMMAND ----------

#Bubble sort
def srt(arr):
  for j in range(len(arr)-1):
    for i in range(len(arr)-1):
      if arr[i]>arr[i+1]:
        arr[i],arr[i+1]=arr[i+1],arr[i]
      
  return arr

a = [22,5,3,5,2,6,7,8,3,1,0]
print(srt(a))

# COMMAND ----------

#Selection Sort: compare current ele with next ones using variable for min index.
#select shortest ele index
def srt1(arr):
  for i in range(len(arr)):
    #set current ele as smallest
    min_idx = i
    #if current ele is greater that next one then update min_idx as j
    for j in range(i+1,len(arr)):
      if arr[min_idx]>arr[j]:
        min_idx = j
        
    arr[i],arr[min_idx] = arr[min_idx],arr[i]
    #if i==2:
    #  break
  return arr
  
a=[4,6,2,1,5,3,7,9,1,0,2]
print(srt1(a))

# COMMAND ----------

#Insertion Sort: move element from back to front one by one(same as cards)
#efficient for small data values
#it is appropriate for data sets which are already partially sorted.
def srt2(arr):
  for i in range(1,len(arr)):
    key = arr[i]
    j=i-1
    
    while j>=0 and key<arr[j]:
      arr[j+1]=arr[j]
      j-=1
    #print(j)
    arr[j+1]=key
    #if i==6:
    #  break
  
  return arr

a = [10,4,8,7,4,2,6,5,0,1]
print(srt2(a))

# COMMAND ----------

def search(arr,x):
  i=1
  while i==1:
    n=len(arr)//2
    #print(arr[n],n)
    if arr[n]==x:
      i=0
      return 'found'
    elif arr[n]>x:
      arr = arr[:n]
      #print(arr)
      continue
    elif arr[n]<x:
      arr = arr[n:]
      #print(arr)
      continue
    
  

a = [1,2,3,4,5,6,7]

print(search(a,4))

# COMMAND ----------

def search1(arr,x):
  l=0
  r = len(arr)-1
  while l<=r:
    mid = l+(r-1)//2
    if arr[mid]==x:
      return mid
    elif arr[mid]<x:
      l=mid+1
    else:
      r = mid-1
  return -1

a = [3,5,4,8,9,5,2,1]
print(search1(a,9))

# COMMAND ----------

# Python3 code to implement iterative Binary
# Search.

# It returns location of x in given array arr
# if present, else returns -1


def binarySearch(arr, l, r, x):

  while l<=r:
    mid = l+(r-1)//2
    if arr[mid]==x:
      return mid
    elif arr[mid]<x:
      l=mid+1
    else:
      r = mid-1
  return -1


# Driver Code
arr = [3,5,4,8,9,5,2,1]
x = 1

# Function call
result = binarySearch(arr, 0, len(arr)-1, 10)

if result != -1:
	print("Element is present at index % d" % result)
else:
	print("Element is not present in array")


# COMMAND ----------

# Python3 code to implement iterative Binary
# Search.

# It returns location of x in given array arr
# if present, else returns -1


def binarySearch(arr, l, r, x):

	while l <= r:

		mid = l + (r - l) // 2

		# Check if x is present at mid
		if arr[mid] == x:
			return mid

		# If x is greater, ignore left half
		elif arr[mid] < x:
			l = mid + 1

		# If x is smaller, ignore right half
		else:
			r = mid - 1

	# If we reach here, then the element
	# was not present
	return -1


# Driver Code
arr = [3,5,4,8,9,5,2,1]
x = 1

# Function call
result = binarySearch(arr, 0, len(arr)-1, x)

if result != -1:
	print("Element is present at index % d" % result)
else:
	print("Element is not present in array")


# COMMAND ----------


