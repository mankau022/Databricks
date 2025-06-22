# Databricks notebook source
class Node:
  def __init__(self,data=None):
    self.data = data
    self.next = None
    
class LList:
  def __init__(self):
    self.head = None
    
  def traverse(self):
    printV = self.head
    while printV:
      print(printV.data)
      printV = printV.next
  
  def insert_begin(self,newdata):
    newnode = Node(newdata)
    newnode.next = self.head
    self.head = newnode
    
  def in_between(self,afternode,newdata):
    V = self.head
    while V:      
      if V.data == afternode:    
        newnode = Node(newdata)
        newnode.next = V.next
        V.next = newnode     
        if V.next is None:
          print('break')
          break
      elif V.next is not None:
        continue
      elif V.next is None:
        print('node is not present')
      V = V.next
      
#LL.head.next.next.next.next.data

# COMMAND ----------

ll = LList()
ll.head = Node('Mon')
e2 = Node('Tue')
ll.head.next = e2


# COMMAND ----------


e3 = Node('Wed')
e2.next = e3

# COMMAND ----------

ll.traverse()

# COMMAND ----------

ll.insert_begin('Sun')

# COMMAND ----------

ll.traverse()

# COMMAND ----------

#Insert value after a particular value
ll.in_between('Wed','Mon') 

# COMMAND ----------

ll.traverse()

# COMMAND ----------

ll.in_between(e2.next,'Fri')

# COMMAND ----------

ll.traverse()

# COMMAND ----------

print(ll.head.next.next.next.next.data)

# COMMAND ----------

print(ll.head.next.next.next.next.next)

# COMMAND ----------



# COMMAND ----------

a = 4
if a>1:
  print(1)
elif a>2:
 print(2)
elif a>3:
  print(3)

# COMMAND ----------


