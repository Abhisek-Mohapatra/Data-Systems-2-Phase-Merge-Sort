import os  # Last Updated : Feb 5, 12:00 am (Final)
import sys
from operator import itemgetter
import heapq
from math import *
from datetime import datetime
import threading

orderOp=''



indexLst=[]  # contains indices of order by columns # indices numbers..


class classHeap(object):
    def __init__(self,lst,part_no):  #lst denotes list of records splitted in a list
        self.lst = lst
        self.part_no=part_no
        

    def __lt__(self, other):
        #return ((self.val) < (other.val))
        #print(self.lst)

        for i in indexLst:
            if(orderOp=='asc'):
                if(self.lst[i]<other.lst[i]):
                    return True
            elif(orderOp=='desc'):
                if(self.lst[i]>other.lst[i]):
                    return True



colDc={}  # stores Bytes corresponding to each column
colNamesLst=[]
readDc={}  # dict to store previous record bytes
recsize=0

def readMetaData(fname):
    global recsize
    f=open('metadata.txt','r')
    content=f.read()
    f.close()
    colLst=content.split('\n')
    totalCol=0
    
    f=open(fname,'r')
    cont=f.readline()
    '''

    if cont.find('\n')>-1:
        print('Hi')
    print(cont.find('\n'))
    print(cont)
    '''
    f.close()
    #tupleSize=len(cont)+1  # '1' is added for extra space after each tuple which it does not consider
    tupleSize=len(cont) # Added Feb 4

    for i in colLst:
        if i=='':
            continue
        totalCol+=1
        rec=i.split(',')
        #print(rec)
        colDc[rec[0]]=int(rec[1])   # eg: colDc['A']=32 (Bytes)
        colNamesLst.append(rec[0])
        if(len(colDc)==1):
            readDc[rec[0]]=0
        
        else:
            val=0
            count=1
            for k in colDc:
                if count==len(colDc):
                    break
                val=val+colDc[k]+2   # 2 is for 2 spaces between the columns
                count+=1
            
            readDc[rec[0]]=val
    

    for i in colDc:
        recsize+=colDc[i]
    
    recsize+=2*(len(colDc)-1)




    '''
    
    print('Heya')
    print(readDc)
    print(colDc)
    '''
    


    

    return tupleSize



def readInputFile(fname):  # return the tuple size

    fileSize=os.path.getsize(fname)
    print('File Size is '+str(fileSize))  # Gets file size in bytes

    f=open(fname,'r')
    '''
    content=f.read()
    f.close()

    lst=content.split('\n')
    total_tuples=len(lst)-1
    '''

    total_tuples=len(f.readlines())
    f.close()
    print('Total tuples is '+str(total_tuples))

    tupleSize=readMetaData(fname)  # We probably get 100 byte tuple with '\n' at the end

    #print('tupleSize is '+str(tupleSize))

    return tupleSize,fileSize

def generatePartitions(tupleSize,tupleCount,mainMemSize,inputFile,fileSize):
    count=0
    part=0
    total_part=fileSize/mainMemSize
    total_part=int(ceil(total_part))
    #print(total_part)

    if(total_part*recsize>mainMemSize):
        sys.exit('Memory Limit Exceeded')

    f=open(inputFile,'r')
    content=''
    

    while part<total_part:
        count=0
        content=''
        while count<tupleCount:
            #msg=f.read(tupleSize-1)
            msg=f.readline()  # added Feb 4
            if msg=='':
                break
            content+=msg
            count+=1
        f1=open('p'+str(part),'w')   # p0,p1,p2  -> 3 partitions
        #content=content.replace('\n',' \n')  #Removed Feb 4
        f1.write(content)
        f1.close()
        part+=1

    f.close()
    return total_part


def sortPartitions(totalPartitions,tupleCount,tupleSize,orderCol,orderOp):

    startPart=0
    #mapDc={}  # maps order by cols to complete record
    colString=''
    #toSortLst=[]
    for j in orderCol:
        indexLst.append(list(colDc.keys()).index(j))  # contains list of indices along which ordering needs to be done

    #indexLst=indLst  # global variable of indexLst updated

    while startPart<totalPartitions:
        print('sorting ####'+str(startPart+1)+' sublist')
        tupleLst=[]
        f=open('p'+str(startPart),'r')
        while True:
            colString=''
            content=f.readline()
            if content=='':
                break
            #content=content.replace('\n',' \n')  #Removed Feb 4
            tempLst=[]

            for i in colDc:
                colString=content[readDc[i]:readDc[i]+colDc[i]]
                tempLst.append(colString)
            
            tupleLst.append(tempLst)
            tempLst=[]
            #indLst=[]
            '''

            for j in orderCol:
                indLst.append(list(colDc.keys()).index(j))  # contains list of indices along which ordering needs to be done
            
            #indLst=tuple(indLst)  # list converted to tuple 

            indexLst=indLst  # global variable of indexLst updated
            '''


            #print(content)
            #print(readDc)
            #print(colDc)
            '''
            for i in orderCol:
                #print(readDc[i])
                #print(colDc[i])
                colString=content[readDc[i]:readDc[i]+colDc[i]]  # string slicing operation
                #print(colString)
            
            toSortLst.append(colString)
            mapDc
            '''
        #print(tupleLst)
        #print(indLst)
        #print(orderOp)
        if(orderOp.lower()=='asc'):
            tupleLst.sort(key=itemgetter(*indexLst))
        elif(orderOp.lower()=='desc'):
            tupleLst.sort(reverse=True,key=itemgetter(*indexLst))
        
        f.close()


        #print(tupleLst)
        print('Writing to disk #'+str(startPart+1))

        f=open('p'+str(startPart),'w')

        for i in tupleLst:
            f.write(('  ').join(i))
            #f.write(' \n')
            f.write('\n')  #added feb4
        f.close()
        
        startPart+=1
        

        #f.close()
    

    
    
    #print(toSortLst)


def transformToLst(data):
    tempLst=[]

    for i in colDc:
        colString=data[readDc[i]:readDc[i]+colDc[i]]
        tempLst.append(colString)

    return tempLst








def processPhaseTwo(totalPartitions,tupleCount,tupleSize,orderCol,orderOp):  # use indexLst #classHeap(lst,part_no)

    print('##running phase-2')
    op_file=open('output.txt','w')

    num=0

    heapQ=[]
    #stLst=[]
    flst=[]
    totalPartitions=int(totalPartitions)

    #for i in range(totalPartitions):
        #stLst.append(i)
    '''
    
    for i in range(totalPartitions):
        fp=open('p'+str(i),'r')
        flst.append(fp)
    '''

    print('Sorting...')


    while(num<totalPartitions):
        fp=open('p'+str(num),'r')
        flst.append(fp)
        #f=open('p'+str(num),'r')
        content=flst[num].readline()

        if(content==''):
            #stLst.remove(num)
            continue
        #content=content.replace('\n',' \n')  # Removed Feb 4

        readLst=transformToLst(content)
        
        heapq.heappush(heapQ, classHeap(readLst,num))

        num+=1
    
    print('Min val extracted..')
    
    num=0

    while(num!=totalPartitions):
        #print('Hi')

        obj=heapq.heappop(heapQ)
        resLst=obj.lst

        #op_file=open('output.txt','a')
        colString=('  ').join(resLst)
        #colString+=' \n'
        colString+='\n'  # Added Feb 4
        #print(len(colString))
        op_file.write(colString)
        #op_file.close()
        
        content=flst[obj.part_no].readline()
        if(content==''):
            num+=1
            continue
        #content=content.replace('\n',' \n')
        readLst=transformToLst(content)

        heapq.heappush(heapQ, classHeap(readLst,obj.part_no))
    
    print('Writing to disk')

    
    for fp in flst:
        fp.close()
    
    op_file.close()


def sortThreadPartitions(countPart,start,end,tupleCount,tupleSize,inputFile,offset):
    #startPart=0
    #mapDc={}  # maps order by cols to complete record
    colString=''
    #toSortLst=[]
    for j in orderCol:
        indexLst.append(list(colDc.keys()).index(j))  # contains list of indices along which ordering needs to be done

    #indexLst=indLst  # global variable of indexLst updated

    while start<end:
        print('sorting ####'+str(start+1)+' sublist')
        tupleLst=[]
        f=open('p'+str(start),'r')
        while True:
            colString=''
            content=f.readline()
            if content=='':
                break
            #content=content.replace('\n',' \n')  #Removed Feb 4
            tempLst=[]

            for i in colDc:
                colString=content[readDc[i]:readDc[i]+colDc[i]]
                tempLst.append(colString)
            
            tupleLst.append(tempLst)
            tempLst=[]
            #indLst=[]
            '''

            for j in orderCol:
                indLst.append(list(colDc.keys()).index(j))  # contains list of indices along which ordering needs to be done
            
            #indLst=tuple(indLst)  # list converted to tuple 

            indexLst=indLst  # global variable of indexLst updated
            '''


            #print(content)
            #print(readDc)
            #print(colDc)
            '''
            for i in orderCol:
                #print(readDc[i])
                #print(colDc[i])
                colString=content[readDc[i]:readDc[i]+colDc[i]]  # string slicing operation
                #print(colString)
            
            toSortLst.append(colString)
            mapDc
            '''
        #print(tupleLst)
        #print(indLst)
        #print(orderOp)
        if(orderOp.lower()=='asc'):
            tupleLst.sort(key=itemgetter(*indexLst))
        elif(orderOp.lower()=='desc'):
            tupleLst.sort(reverse=True,key=itemgetter(*indexLst))
        
        f.close()

        

        #print(tupleLst)
        print('Writing to disk #'+str(start+1))

        f1=open('p'+str(start),'w')

        for i in tupleLst:
            f1.write(('  ').join(i))
            #f1.write(' \n')  # Removed Feb 4
            f1.write('\n')
        f1.close()
        
        start+=1
        

        #f.close()




def generateThreadPartitions(countPart,start,end,tupleCount,tupleSize,inputFile,offset):

    

    if(start>=countPart):
        return 

    f=open(inputFile,'r')
    f.seek(offset)
    content=''
    temp=start
    

    while start<end:
        count=0
        content=''
        while count<tupleCount:
            #msg=f.read(tupleSize-1)
            msg=f.readline()  # Added Feb 4
            if msg=='':
                break
            content+=msg
            count+=1
        f1=open('p'+str(start),'w')   # p0,p1,p2  -> 3 partitions
        #content=content.replace('\n',' \n')  #Removed Feb 4
        f1.write(content)
        f1.close()
        start+=1

    f.close()
    start=temp

    sortThreadPartitions(countPart,start,end,tupleCount,tupleSize,inputFile,offset)
    #return total_part

    




def processPhaseOne(inputFile,outputFile,mainMemSize,orderOp,orderCol,threadCount):

    
    print('#### start execution')

    begin_time=datetime.now()
    print(begin_time)

    print('### running Phase-1')
    

    tupleSize,fileSize=readInputFile(inputFile)
    #print('rec size is:',recsize)
    mainMemSize=int(mainMemSize)
    mainMemSize=mainMemSize*pow(10,6)  
    #mainMemSize=mainMemSize

    tupleCount=int(floor(mainMemSize/tupleSize)) # denotes the number of tuples that can fit in main memory at once
    totalPartitions=0

    if(threadCount>0):  # if thread count is given in the question
        threadLst=[]
        no_partitions=int(ceil(fileSize/mainMemSize))

        if(no_partitions*recsize>mainMemSize):
            sys.exit('Memory Limit Exceeded')

        totalPartitions=no_partitions
        if(no_partitions<=threadCount):
            start=0
            end=1
            offset=0
        
            for i in range(threadCount):
                if offset>fileSize:
                    break
                t=threading.Thread(target=generateThreadPartitions,args=(no_partitions,start,end,tupleCount,tupleSize,inputFile,offset,))
                threadLst.append(t)
                t.start()
                offset+=(end-start)*mainMemSize
                start+=1
                end+=1

            for t in threadLst:
                t.join()

        else:
            
            t_part=int(ceil(no_partitions/threadCount))
            start=0
            end=t_part
            offset=0
            for i in range(threadCount):
                if offset>fileSize:
                    break
                t=threading.Thread(target=generateThreadPartitions,args=(no_partitions,start,end,tupleCount,tupleSize,inputFile,offset,))
                threadLst.append(t)
                t.start()
                offset+=(end-start)*mainMemSize
                start=end
                end+=t_part

            for t in threadLst:
                t.join()
        
        print('Number of sub-files (splits): '+str(totalPartitions))



    else:

        totalPartitions=generatePartitions(tupleSize,tupleCount,mainMemSize,inputFile,fileSize)
        totalPartitions=ceil(totalPartitions)

        print('Number of sub-files (splits): '+str(totalPartitions))
    
    if threadCount==0:
        sortPartitions(totalPartitions,tupleCount,tupleSize,orderCol,orderOp)


    processPhaseTwo(totalPartitions,tupleCount,tupleSize,orderCol,orderOp)
    

    print('###completed execution')

    for i in range(totalPartitions):
        os.remove('p'+str(i))

    end_time=datetime.now()
    print(end_time)

    print('Time elapsed: '+str(end_time-begin_time))



if(len(sys.argv)<6):
    sys.exit('Insufficient parameters passed')


threadCount=0
inputFile=sys.argv[1]
outputFile=sys.argv[2]
mainMemSize=sys.argv[3]   # '50 MB'  


if(sys.argv[4].isdigit()):
    threadCount=int(sys.argv[4])
    orderOp=sys.argv[5]    # asc | desc
    orderCol=sys.argv[6:]  # ['col1','col2']
    processPhaseOne(inputFile,outputFile,mainMemSize,orderOp,orderCol,threadCount)

else:    
    orderOp=sys.argv[4]    # asc | desc

    orderCol=sys.argv[5:]  # ['col1','col2']

    processPhaseOne(inputFile,outputFile,mainMemSize,orderOp,orderCol,threadCount)
  

