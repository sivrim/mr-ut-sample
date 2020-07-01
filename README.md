# Sample unit tests for map-reduce with Hadoop jars 2.6.0, mockito 3.3.3 and Junit 5.4.2. 

While setting up my unit test environment, assembling the right versions of depdencies and writing the first set of tests took a bit of an effort. Hopefully this can help someone the near future, i.e. next couple of years. 

Driver class is left out on purpose.

Steps : 
1. Write any custom Writable class you need
1. Write tests for custom Writable class if needed
1. Create Mapper class.
2. Write Test for mapper
1. Write mapper logic, test, fix, repeat
1. Repeat above 3 steps for combiners and reducers
