# Databricks notebook source
# MAGIC %md
# MAGIC <img src = 'files/Class_Variables.PNG'>"

# COMMAND ----------

class Person:
  #class variable
  name='Manish'
  
  def __init__(self,age):
    self.age=age#instance variables are defined in constructor method
    #print(f"Object has initiated") 
  
  def get(self):
    return self.name,Person.name#Person.name returns value defined in class and self.name returns value defined while creating object.

# COMMAND ----------

# MAGIC %md
# MAGIC if one of the objects modifies the value of a class variable, then all objects start referring to the fresh copy.

# COMMAND ----------

a = Person(29)
b = Person(30)
c = Person(30)
a.name='Vishal'#a new instance variable is created for object 'a', which shadows the class variables. So always use the class name to modify the class variable.
#Person.name='Somya'#Use to actually update class variable value.
b.name='Somya'

print(a.get(),a.name,a.age)
print(b.get(),Person.name,b.name,b.age)#Person.name returns value defined in class and b.name returns value defined while creating object.
print(c.get(),Person.name,c.age)#cannot use Person.age as age was passed while creating object.

# COMMAND ----------

# MAGIC %md
# MAGIC ###init and Inheritance
# MAGIC The __init__ method is similar to constructors in C++ and Java. It is run as soon as an object of a class is instantiated. The method is useful to do any initialization you want to do with your object. 

# COMMAND ----------

class Person1:
  gender = 'Female'
  def __init__(self,name,age):
    self.name=name
    self.age=age
  
  def get(self):
    print(f'My name is {self.name}, I am a {self.gender} and age is {self.age}')
  
  def getFunc(self): 
    print('Calling Parent class function from Child object')

# COMMAND ----------

c = Person1('Manish','10')
c.gender = 'Male'#instance variable
c.get()

# COMMAND ----------

class Male(Person1):#to get Parent class methods, call here
  #class variable: do not vary from object to object
  gender = 'Male'#overrides parent class 'class variable'.
  
  #instance variable are defined in constructor method or __init__
  def __init__(self,name,age,prof):
    self.prof=prof#instance variable: different for every instance or object
    
    Person1.__init__(self,name,age)#to get Parent class variable, call constructor here
  
  def get(self):
    print(f'My name is {self.name}({self.gender}), age {self.age} and I am a {self.prof}')
    
  def getName(self):
    print(f'My Name is {self.name}')

# COMMAND ----------

d = Male('Somya',15,'Backend Developer')
d.gender='Female'#comment and try
d.get() #returns Female which I just set above
d.getFunc()

# COMMAND ----------

d.__class__.gender #returns what is present in class

# COMMAND ----------

# MAGIC %md
# MAGIC ###Polymorphism 
# MAGIC Polymorphism means the same function name (but different signatures) being used for different types.  
# MAGIC Polymorphism lets us define methods in the child class that have the same name as the methods in the parent class. In inheritance, the child class inherits the methods from the parent class. However, it is possible to modify a method in a child class that it has inherited from the parent class. This is particularly useful in cases where the method inherited from the parent class doesn’t quite fit the child class. In such cases, we re-implement the method in the child class. This process of re-implementing a method in the child class is known as 'Method Overriding'.  

# COMMAND ----------

class Child(Male):
  age=0
  
  def get(self):
    print(f'I am {self.name}')
    print(f'Age: {self.age}')

# COMMAND ----------

e = Child('Naimish',1,'NA')
e.age=10 #if age value is not set here, age value(1) will come from parent class
e.get() #overrides the parent class method
e.getName() 

# COMMAND ----------

class Animal:
	def speak(self):
		raise NotImplementedError("Subclass must implement this method")

class Dog(Animal):
	def speak(self):
		return "Woof!"

class Cat(Animal):
	def speak(self):
		return "Meow!"

# Create a list of Animal objects
animals = [Dog(), Cat()]

# Call the speak method on each object
for animal in animals:
	print(animal.speak())

# COMMAND ----------

print(issubclass(Dog,Animal))
print(issubclass(Animal,Dog))
print(isinstance(e,Child))
print(isinstance(e,Male)) #instance of parentclass

# COMMAND ----------

# MAGIC %md
# MAGIC ###Data hiding  
# MAGIC we use double underscore (Or __) before the attributes name and those attributes will not be directly visible outside.  
# MAGIC Private methods are accessible outside their class, just not easily accessible. Nothing in Python is truly private; internally, the names of private methods and attributes are mangled and unmangled on the fly to make them seem inaccessible by their given names

# COMMAND ----------

class MyClass:
  __hiddenvar1 = 10 
  def __init__(self,__hiddenvar2):
    self.__hiddenvar2=__hiddenvar2
  
  def __str__(self):
    return 'var1 is {} \nvar2 is {}'.format(self.__hiddenvar1,self.__hiddenvar2)
  
  def __repr__(self):
    return 'From repr method of MyClass, var1: {} and var2: {}'.format(self.__hiddenvar1,self.__hiddenvar2)          
  
  def __get(self):#hide function also
    print(f'var1: {self.__hiddenvar1}')
    print(f'var2: {self.__hiddenvar2}')     
        
o = MyClass(1)
#print(o.__hiddenvar1) #throws error
print(o._MyClass__hiddenvar1) #Need to change attribute name to access it outside
o._MyClass__get() #can be used within class with same name

# COMMAND ----------

print(o) #__str__
print([o]) #If no __str__ method is defined, print (o) uses __repr__

# COMMAND ----------

# MAGIC %md
# MAGIC ###MultiInheritance

# COMMAND ----------

# Python example to show working of multiple
# inheritance
class Base1:
  def __init__(self,str1):
    self.str1 = str1
    print('Base1')
    
class Base2:
  def __init__(self,str2):
    self.str2=str2
    print('Base2')
  
  def get(self):
    print('Base2: get function')
    
class Derived(Base1,Base2):#to access functions, pass Parent class here
  def __init__(self,string1,string2):
    self.string1=string1#use self.string1(child)
    self.string2=string2
    #Base1.str1=string1
    #Base2.str2=string2
    #super().__init__(string1)#Both super are calling Base1 constructor
    #super().__init__(string2)#overrides above super   
    Base1.__init__(self,string1)#to access attributes, call parent class constructor here
    Base2.__init__(self,string2)
    print('Derived')
    
  def printStrs(self):
    print('Parents:',self.str1,self.str2)
    print('Derived:',self.string1,self.string2)
    #print('Parents:',Base1.string1,Base2.string2)
    
ob = Derived('Geek1','Geek2')
ob.printStrs()
ob.get()

# COMMAND ----------

# MAGIC %md
# MAGIC ###super() in multiple inheritance

# COMMAND ----------


