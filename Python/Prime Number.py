# Databricks notebook source
l = []
for i in range(1,100):
  if i>1:
    for j in range(2,i-1):
      if i%j==0:
        break
    else:
      l.append(i)
print(l)

# COMMAND ----------

n=11
if n>1:
  for i in range(2,n-1):
    if n%i==0:
      print('not prime')
      break
  else:
    print('1 prime')
else:
  print('not prime')

# COMMAND ----------


