#!/usr/bin/python

#Brandon O'Hare, Socket Group 44
import socket
import sys
import select
import csv
from time import sleep
import shlex

#Acceptable Port Nums for Group: (23000, 23499)
#Initializing variables from command line input, initializing socket
#------------------------------------------------------------------------------------------------------
serverIP = sys.argv[1]
port = int(sys.argv[2])
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.setblocking(1)

#Function to get the position in the hashTable where a long name should be placed
#------------------------------------------------------------------------------------------------------
def getPOS(word):
    temp = list(word)
    position = 0
    for i in range(0,len(temp)):
        val = int(ord(temp[i]))
        position = position + val
    return position % 353

#Function to get the ID of user based on the position and the number of users
#------------------------------------------------------------------------------------------------------
def getID(value, num):
    return value % num
#------------------------------------------------------------------------------------------------------
def main():

#Initializing globals
#------------------------------------------------------------------------------------------------------
    state = None                        #State of this client
    leftNeighbor = None                 #3 tuple of the left neighbor for this client
    rightNeighbor = None                #3 tuple of the right neighbor for this client
    id = 0                              #ID for this client
    numUsers = 0                        #Number of users in the DHT
    leaderArr = [None]                  #Array used by the leader which contains all the users in the DHT
    hashTable = [None] * 353            #Hash table
    input = [s, sys.stdin]              #Used by .select() to nonblock based on stdin and socket data
    host_ip = socket.gethostbyname(socket.getfqdn())    #IP of this client
    print "Host IP", host_ip            

#Infinite loop, utilizing nonblocking input via .select()
#------------------------------------------------------------------------------------------------------
    while True:
        try:
            inputready, outputready, exceptready = select.select(input, [], [])     #Check for data from stdin and socket
            for x in inputready:

#If the data recieved is stdin
#------------------------------------------------------------------------------------------------------
                if x == sys.stdin:                                 
                    command = sys.stdin.readline()                  #Reading line from stdin
                    command = shlex.split(command)                  #Splitting command based on white space, preserving commands encapsulated by quotes
                    print("Command:", command[0])
                    destination = (serverIP, port)                  #Setting the tuple used for communicating with the server

#Handles the register commmand
#------------------------------------------------------------------------------------------------------
                    if(command[0] == "register") and (len(command) == 3):
                        print("Register command")
                        s.bind(('', int(command[2])))               #Binding port based on stdin
                        s.sendto(command[0].encode(),destination)   #Sending register
                        s.sendto(command[1].encode(),destination)   #Sending username
                        s.sendto(host_ip.encode(),destination)      #Sending the client's IP
                        s.sendto(command[2].encode(),destination)   #Sending the client's chosen port
                        reply = s.recvfrom(1024)                    #Waiting for reply from server
                        if reply[0] == "SUCCESS":
                            state = "Free"
                        print(reply[0])          

#Handles the setup-dht command
#------------------------------------------------------------------------------------------------------           
                    if(command[0] == "setup-dht") and (len(command) == 3):
                        print("setup command")
                        s.sendto(command[0].encode(),destination)   #Sending setup-dht
                        s.sendto(command[1].encode(),destination)   #Sending desired number of users
                        s.sendto(command[2].encode(),destination)   #Sending leader name
                        reply = s.recvfrom(1024)                    #Waiting on server reply
                        numUsers = int(command[1])                  #Storing the number of users for later propagation
                        print(reply[0])                                         
                        if reply[0] == "SUCCESS":
                            state = "Leader"
                            data = s.recvfrom(1024)                 #Waiting on server to send users for DHT

#Based on the number of users needed for DHT, loop collects users for the leader list 
#------------------------------------------------------------------------------------------------------
                            if data[0].decode() == "set-id-return":
                                for i in range(0, int(command[1])):
                                    userID = s.recvfrom(1024)       #Waiting for a user's ID
                                    userIP = s.recvfrom(1024)       #Waiting for a user's IP
                                    userPort = s.recvfrom(1024)     #Waiting for a user's port
                                    newUser = (userID[0],userIP[0],userPort[0])
                                    leaderArr.append(newUser)       #Adding user to list

#Initializing ID and neighbors for the leader of the DHT
#------------------------------------------------------------------------------------------------------
                                numID = 0
                                id = numID
                                leftNeighbor = leaderArr[len(leaderArr)-1]
                                rightNeighbor = leaderArr[2]

#Sends each user their given left and right neighbors, along with the number of users and their ID
#------------------------------------------------------------------------------------------------------
                                for i in range(2, len(leaderArr)):
                                    numID += 1      #Each user has a higher number for ID
                                    currentDest = (leaderArr[i][1],int(leaderArr[i][2])) #Tuple for sending to next user
                                    s.sendto("setting-neighbor".encode(), currentDest)  #Sending setting-neighbor
#Sending left neighbor
#------------------------------------------------------------------------------------------------------                                  
                                    s.sendto(leaderArr[i-1][0].encode(), currentDest)   #Sending userName
                                    s.sendto(leaderArr[i-1][1].encode(), currentDest)   #Sending userIP
                                    s.sendto(leaderArr[i-1][2].encode(), currentDest)   #Sending userPort
                                    s.sendto(str(numID).encode(), currentDest)          #Sending IDnum 
                                    s.sendto(command[1].encode(),currentDest)           #Sending numUsers
#Sending right neighbor
#------------------------------------------------------------------------------------------------------
                                    if i < len(leaderArr)-1:
                                        s.sendto(leaderArr[i+1][0].encode(), currentDest)
                                        s.sendto(leaderArr[i+1][1].encode(), currentDest)
                                        s.sendto(leaderArr[i+1][2].encode(), currentDest)
                                        s.sendto(str(numID).encode(), currentDest)
                                        s.sendto(command[1].encode(),currentDest)
#Sending right neighbor as the leader if we've reached the end
#------------------------------------------------------------------------------------------------------
                                    else:
                                        s.sendto(leaderArr[1][0].encode(), currentDest)
                                        s.sendto(leaderArr[1][1].encode(), currentDest)
                                        s.sendto(leaderArr[1][2].encode(), currentDest)
                                        s.sendto(str(numID).encode(), currentDest)
                                        s.sendto(command[1].encode(),currentDest)

#Handling the reading of the CSV file
#------------------------------------------------------------------------------------------------------ 
                            with open ('StatsCountry.csv') as csv_file:
                                csv_reader = csv.reader(csv_file, delimiter=',')
                                count = 0
                                for row in csv_reader:
                                    if count == 0:
                                        count += 1
                                    else:
                                        count += 1
#Checking if the data belongs in the Leader's hash table                                        
#------------------------------------------------------------------------------------------------------
                                        position = getPOS(row[3])
                                        idCheck = getID(position, numUsers)
                                        if idCheck == 0:
                                            if hashTable[position] == None:

                                                hashTable[position] = row
                                            else:
                                                temp = position + 1
                                                while hashTable[temp] != None:
                                                    if temp == 352:
                                                        temp = -1
                                                    temp = temp + 1
                                                hashTable[temp] = row
#Sending it to the right neighbor if it doesn't belong with Leader
#------------------------------------------------------------------------------------------------------
                                        else: 
                                            currentDest = (rightNeighbor[1], int(rightNeighbor[2]))
                                            s.sendto("store".encode(), currentDest)
                                            for i in range(0,len(row)):
                                                s.sendto(row[i].encode(), currentDest)
                                                sleep(.0005)        #Slight delay needed for clients to stay in sync
                            print "FINISHED DHT BUILD"
                            state = "InDHT"

#Function to teardown the entire DHT
#------------------------------------------------------------------------------------------------------
                    if(command[0] == "teardown-dht") and (len(command) == 2):
                        if state == "Free":                 #Only InDHT and Leader can teardown
                            "FREE USERS CANNOT TEARDOWN"
                        else:
                            s.sendto(command[0].encode(),destination)       #Sending teardown-dht
                            s.sendto(command[1].encode(),destination)       #Sending username 
                            s.sendto(str(numUsers).encode(),destination)    #Sending number of users
                            
#Function for user to leave DHT, utilizes teardown framework
#------------------------------------------------------------------------------------------------------
                    if(command[0] == "leave-dht") and (len(command) == 2):
                        if (state == "Leader") or (state == "Free"):
                            print "CANNOT LEAVE DHT"
                        else:
                            s.sendto(command[0].encode(),destination)       #Sending leave-dht
                            s.sendto(command[1].encode(),destination)       #Sending username 
                            s.sendto(str(numUsers).encode(),destination)    #Sending number of users

#Function to deregister the user from the server, cannot re-register after deregistering                       
#------------------------------------------------------------------------------------------------------
                    if(command[0] == "deregister") and (len(command) == 2):
                        if state == "Free":
                            s.sendto(command[0].encode(),destination)       #Sending deregister
                            s.sendto(command[1].encode(),destination)       #Sending user
                            reply = s.recvfrom(1024)
                            print reply[0]
                            if reply[0] == "SUCCESS":
                                state = None
                        else:
                            print "FAILURE"

#Function to notify server that DHT has finished building
#------------------------------------------------------------------------------------------------------
                    if(command[0] == "dht-complete") and (len(command) == 2):
                        s.sendto(command[0].encode(),destination)           #Sending dht-complete
                        s.sendto(command[1].encode(),destination)           #Sending user
                        reply = s.recvfrom(1024)
                        print reply[0]
    
#Function to ask server which client to query
#------------------------------------------------------------------------------------------------------
                    if(command[0] == "query-dht") and (len(command) == 2):
                        s.sendto(command[0].encode(),destination)           #Sending query-dht
                        s.sendto(command[1].encode(),destination)           #Sending user
                        reply = s.recvfrom(1024)
                        print reply[0]
                        if reply[0] == "SUCCESS":
                            querIP = s.recvfrom(1024)       #Waiting for IP to query
                            querPort = s.recvfrom(1024)     #Waiting for port to query
                            print "Query this user: "
                            print querIP[0]
                            print querPort[0]
                        
#Function to send query command to a client in the DHT 
#------------------------------------------------------------------------------------------------------
                    if(command[0] == "query") and (len(command) == 4):
                        querDest = (command[2], int(command[3]))        #Client with DHT
                        s.sendto(command[0].encode(),querDest)          #Sending query
                        s.sendto(command[1].encode(),querDest)          #Sending long name for query

#Function to check state info for debugging
#------------------------------------------------------------------------------------------------------
                    if(command[0] == "check"):
                        print("check")
                        print "state", state
                        print "left neighbor", leftNeighbor
                        print "right neighbor", rightNeighbor
                        print "id", id
                        print "num users", numUsers

#Function to list everything inside the hash, count number of hash items
#------------------------------------------------------------------------------------------------------
                    if(command[0] == "check-hash"):
                        count = 0
                        for i in range(0,353):
                            if hashTable[i] != None:
                                count += 1
                                print hashTable[i]
                        print "count", count

#Function to exit the program, followed by ^C to close the window
#------------------------------------------------------------------------------------------------------
                    if(command[0]) == "exit":
                        exit()


#######################################################################################################

#If the data is received from the socket
#------------------------------------------------------------------------------------------------------
                else:
                    
                    data = s.recvfrom(1024)                         #Waiting for data

#Function for handling setting of neighbors, as given from the left neighbor
#------------------------------------------------------------------------------------------------------
                    if data[0].decode() == "setting-neighbor":
                        for i in range(0, 2):
                                neighborName = s.recvfrom(1024)     #Waiting for name of neighbor
                                neighborIP = s.recvfrom(1024)       #Waiting for IP of neighbor
                                neighborPort = s.recvfrom(1024)     #Waiting for port of neighbor
                                idNum = s.recvfrom(1024)            #Waiting for ID
                                numUsers = s.recvfrom(1024)         #Waiting for num of users
                                numUsers = int(numUsers[0])         #Setting num as int
                                if i == 0:  #Setting left neighbor
                                    leftNeighbor = (neighborName[0], neighborIP[0], neighborPort[0])
                                else:       #Setting right neighbor
                                    rightNeighbor = (neighborName[0], neighborIP[0], neighborPort[0])
                                id = int(idNum[0])

#Function for storing data into hashTable as given from left neighbor
#------------------------------------------------------------------------------------------------------
                    if data[0].decode() == "store":
                        state = "InDHT" 
                        record = []
                        for i in range(0,9):
                            temp = s.recvfrom(1024)
                            record.append(temp[0].decode())

                        position = getPOS(record[3])        #Getting position from long name
                        idCheck = getID(position, numUsers) #Getting ID from position and num users
                        if id == idCheck:   #ID matches, place at this hash table
                            if hashTable[position] == None:     #Space is empty
                                hashTable[position] = record
                            else:                               #Space isn't empty, continue forward to avoid collision and place
                                temp = position + 1
                                while hashTable[temp] != None:
                                    if temp == 352:
                                        temp = -1
                                    temp = temp + 1
                                hashTable[temp] = record

                        if (id != idCheck) and (id == numUsers-1):  #ID never matched, and we've reached the end of the DHT ring
                            print "ID:", id
                            print "IDCHECK", idCheck
                            print "NUM USERS", numUsers
                            print "RECORD NOT PLACED"

                        if (id < idCheck):  #Send it to the right neighbor to propagate
                            currentDest = (rightNeighbor[1], int(rightNeighbor[2]))
                            s.sendto("store".encode(), currentDest)
                            for i in range(0,9):
                                s.sendto(record[i].encode(), currentDest)

#Function to handle a query to the LEADER              
#------------------------------------------------------------------------------------------------------
                    if data[0].decode() == "query":
                        longName = s.recvfrom(1024)                 #Waiting for long name
                        longName = longName[0].decode()
                        pos = getPOS(longName)                      
                        if hashTable[pos] != None:  #Space isn't empty               
                            if hashTable[pos][3] == longName:   #Correct record is present, send to the client making query
                                s.sendto("record-info".encode(),data[1])
                                s.sendto("FOUND".encode(),data[1])
                                for j in range(0,9):    #Send entire record
                                    s.sendto(hashTable[pos][j].encode(),data[1])
                            else:   #Correct record is not present in spot
                                foundFlag = False
                                for i in range(0, 353): #Check entire hash to make sure it wasn't placed somewhere else to avoid collision
                                    if hashTable[i] != None:
                                        if hashTable[i][3] == longName:
                                            foundFlag = True
                                            s.sendto("record-info".encode(),data[1])
                                            s.sendto("FOUND".encode(),data[1])
                                            for j in range(0,9):
                                                s.sendto(hashTable[i][j].encode(),data[1])

                                if (id != (numUsers-1)) and (foundFlag == False):   #Wasn't found, send query to right neighbor
                                    dest = (rightNeighbor[1], int(rightNeighbor[2]))
                                    s.sendto("query-two".encode(), dest)
                                    s.sendto(longName.encode(), dest)
                                    s.sendto(data[1][0].encode(), dest)
                                    s.sendto(data[1][1].encode(), dest)

                                if (id == numUsers-1) and (foundFlag == False):     #Wasn't found, reached end of ring
                                    s.sendto("record-info".encode(),data[1])
                                    s.sendto("NOT FOUND".encode(),data[1])

                        else:   #Space in position is empty
                            if id != numUsers-1: #Send it to right neighbor
                                dest = (rightNeighbor[1], int(rightNeighbor[2]))
                                s.sendto("query-two".encode(), dest)
                                s.sendto(longName.encode(), dest)
                                s.sendto(str(data[1][0]), dest)
                                s.sendto(str(data[1][1]).encode(), dest)
                            else:   #Wasn't found, reached end of ring
                                s.sendto("record-info".encode(),data[1])
                                s.sendto("NOT FOUND".encode(),data[1])

#Function for querying if client is NOT LEADER
#------------------------------------------------------------------------------------------------------
                    if data[0].decode() == "query-two":
                        longName = s.recvfrom(1024)     #Waiting for the long name
                        custIP = s.recvfrom(1024)       #Waiting for the IP of the client querying
                        custPort = s.recvfrom(1024)     #Waiting for the port of the client querying

                        pos = getPOS(longName[0])
                        if hashTable[pos] != None:  #Space isn't empty 
                            if hashTable[pos][3] == longName[0]:  #Correct record is present, send to the client making query
                                customer = (custIP[0], int(custPort[0]))
                                s.sendto("record-info".encode(),customer)
                                s.sendto("FOUND".encode(),customer)
                                for j in range(0,9):    #Send entire record
                                    s.sendto(hashTable[pos][j].encode(),customer)
                            else:           #Correct record is not present in spot
                                foundFlag = False
                                for i in range(0, 353): #Check entire hash to make sure it wasn't placed somewhere else to avoid collision
                                    if hashTable[i] != None:
                                        if hashTable[i][3] == longName[0]:
                                            foundFlag = True
                                            customer = (custIP[0], int(custPort[0]))
                                            s.sendto("record-info".encode(),customer)
                                            s.sendto("FOUND".encode(),customer)
                                            for j in range(0,9):
                                                s.sendto(hashTable[i][j].encode(),customer)

                                if (id != numUsers-1) and (foundFlag == False): #Wasn't found, send query to right neighbor
                                    dest = (rightNeighbor[1], int(rightNeighbor[2]))
                                    s.sendto("query-two".encode(), dest)
                                    s.sendto(longName[0].encode(), dest)
                                    s.sendto(custIP[0].encode(), dest)
                                    s.sendto(custPort[0].encode(), dest)
                                if (id == numUsers-1) and (foundFlag == False): #Wasn't found, reached end of ring
                                    customer = (custIP[0], int(custPort[0]))
                                    s.sendto("record-info".encode(),customer)
                                    s.sendto("NOT FOUND".encode(),customer)
                        else:       #Space in position is empty
                            if id != (numUsers-1):  #Send it to right neighbor
                                dest = (rightNeighbor[1], int(rightNeighbor[2]))
                                s.sendto("query-two".encode(), dest)
                                s.sendto(longName[0].encode(), dest)
                                s.sendto(custIP[0].encode(), dest)
                                s.sendto(custPort[0].encode(), dest)
                            else:        #Wasn't found, reached end of ring
                                customer = (custIP[0], int(custPort[0]))
                                s.sendto("record-info".encode(),customer)
                                s.sendto("NOT FOUND".encode(),customer)

#Function to receive and print the status of the queried record
#------------------------------------------------------------------------------------------------------
                    if data[0].decode() == "record-info":
                        response = s.recvfrom(1024)
                        if response[0].decode() == "FOUND":
                            print "FOUND:"
                            information = []
                            for i in range(0,9):
                                temp = s.recvfrom(1024)
                                information.append(temp[0].decode())
                            print information

                        else:
                            print "NOT FOUND"
                    
#Function to teardown the DHT, reseting state variables and sending finish message after propagating through entire ring
#------------------------------------------------------------------------------------------------------
                    if data[0].decode() == "teardown":
                        state = "Free"
                        leaderArr = [None]
                        hashTable = [None] * 353
                        leftNeighbor = None
                        if id != numUsers-1:
                            dest = (rightNeighbor[1], int(rightNeighbor[2]))
                            s.sendto("teardown".encode(),dest)
                        else:
                            s.sendto("finished-teardown".encode(),destination)
                        rightNeighbor = None
                        id = 0
                        numUsers = 0

#Function to prompt reset of the DHT
#------------------------------------------------------------------------------------------------------
                    if data[0].decode() == "reset-dht":
                        numUser = s.recvfrom(1024)
                        user = s.recvfrom(1024)
                        print "USER HAS LEFT THE DHT"
                        print "ENTER: setup-dht", numUser[0], user[0]

#Error handling
#------------------------------------------------------------------------------------------------------                 
        except:
            pass

#Main function
#------------------------------------------------------------------------------------------------------ 
main()
