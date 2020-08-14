#!/usr/bin/python

#Brandon O'Hare, Socket Group 44
import socket
import sys
import time

#Global variables
#------------------------------------------------------------------------------------------------------
users = []
userStates = []
port = int(sys.argv[1])
leader = None

#Function to find the index of a user via name
#------------------------------------------------------------------------------------------------------
def findIndex(List, item):
    itemLocation = None
    for i in range(0, len(List)):
        if List[i][0] == item:
            itemLocation = i
    return itemLocation

#Function to check if the given user index is the leader
#------------------------------------------------------------------------------------------------------
def checkLeader(List, index):
    flag = None
    if index == None:
        flag == False
    else:
        if List[index] == "Leader":
            flag = True
        else:
            flag = False
    return flag


def main():

#More globals, binding socket to port that was given in command line
#------------------------------------------------------------------------------------------------------
    dhtFlag = False
    computeDHT = False
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(('', port))
    print "Server listening on port %s" %(port)

#Infinite loop waiting for client data
#------------------------------------------------------------------------------------------------------
    while True:

        command = s.recvfrom(1024)  #Waiting on a command

#Function to handle the registering of a user
#------------------------------------------------------------------------------------------------------
        if (command[0].decode() == "register") and (computeDHT == False):
            print "register command triggered"
            userName = s.recvfrom(1024)
            if not(any(userName[0] in sublist for sublist in users)):   #Checking that user doesn't already exist
                userIP = s.recvfrom(1024)
                userPort = s.recvfrom(1024)
                users.append((userName[0], userIP[0], userPort[0]))
                userStates.append("Free")
                reply = "SUCCESS"
                s.sendto(reply.encode(), command[1])
            else:
                reply = "FAILURE"
                s.sendto(reply.encode(), command[1])

#Function to set up the DHT as send by leader client
#------------------------------------------------------------------------------------------------------    
        if (command[0].decode() == "setup-dht") and (computeDHT == False):
            print("setup-dht command triggered")
            numMembers = s.recvfrom(1024)
            userName = s.recvfrom(1024)
            if(int(numMembers[0]) < len(users) and any(userName[0] in sublist for sublist in users) and (int(numMembers[0] > 2))): #Check for enough users, if user exists, and that num is over 2
                reply = "SUCCESS"
                s.sendto(reply.encode(), command[1])
                leader = userName[0]    #Setting user as leader
                userStates[findIndex(users, leader)] = "Leader" #Saving leader state in list
                s.sendto("set-id-return".encode(), command[1])  #Sending command to leader
                sendLead = users[findIndex(users, leader)]
                s.sendto(sendLead[0], command[1])
                s.sendto(sendLead[1], command[1])
                s.sendto(sendLead[2], command[1])
                countUsers = 0
                loopVal = 0
                while countUsers != (int(numMembers[0])-1): #While we havent sent enough users, send more users
                    if userStates[loopVal] == "Free":   #Only send if they are free
                        sendUsers = users[loopVal]
                        s.sendto(sendUsers[0], command[1])
                        s.sendto(sendUsers[1], command[1])
                        s.sendto(sendUsers[2], command[1])
                        countUsers += 1 
                        if userStates[loopVal] == "Free":   #Set free users as computing
                            userStates[loopVal] = "Computing"
                    loopVal += 1
                computeDHT = True   #DHT is being computed currently
                
            else:
                reply = "FAILURE"
                s.sendto(reply.encode(), command[1])

#Function to complete DHT, and allow for queries again
#------------------------------------------------------------------------------------------------------
        if (command[0].decode() == "dht-complete") and (computeDHT == True):
            check = s.recvfrom(1024)
            loc = findIndex(users, check[0].decode())
            if checkLeader(userStates, loc) == True: #Checking that user is in fact the leader
                s.sendto("SUCCESS".encode(),command[1])
                dhtFlag = True
                for i in range(0, len(userStates)): #Moving users to InDHT state
                    if userStates[i] == "Computing":
                        userStates[i] = "InDHT"
                computeDHT = False
            else:
                s.sendto("FAILURE".encode(),command[1])
            for i in range(0, len(userStates)): #Unblocking blocked users (used for leave-dht command)
                if userStates[i] == "Blocked":
                    userStates[i] == "Free"

#Function to deregister a user
#------------------------------------------------------------------------------------------------------
        if (command[0].decode() == "deregister") and (computeDHT == False):
            userDel = s.recvfrom(1024)
            ind = findIndex(users, userDel[0])
            if (ind != None) and (userStates[ind] == "Free"):
                del users[ind]
                del userStates[ind]
                s.sendto("SUCCESS".encode(),command[1])
            else:
                s.sendto("FAILURE".encode(),command[1])

#Function to send a querying client the leader's information
#------------------------------------------------------------------------------------------------------
        if (command[0].decode() == "query-dht") and (computeDHT == False):
            user = s.recvfrom(1024)
            user = user[0].decode()
            search = findIndex(users,user)
            if (search != None) and (dhtFlag == True):
                if userStates[search] == "Free":    #If the user is free, and the DHT is built, send them the leader's address
                    s.sendto("SUCCESS".encode(),command[1])
                    lead = users[findIndex(users, leader)]
                    s.sendto(lead[1].encode(),command[1])
                    s.sendto(lead[2].encode(),command[1])
                else:
                    s.sendto("FAILURE".encode(),command[1])
            else:
                s.sendto("FAILURE".encode(),command[1])

#Function to handle both the teardown of the DHT and a client leaving the DHT
#------------------------------------------------------------------------------------------------------
        if ((command[0].decode() == "teardown-dht") or (command[0].decode() == "leave-dht")) and (computeDHT == False):
            blockUser = s.recvfrom(1024)    #User asking to leave
            numUser = s.recvfrom(1024)      #Number of users
            sendLead = users[findIndex(users, leader)]  #Leader, which will be sent the teardown command
            dest = (sendLead[1], int(sendLead[2]))
            s.sendto("teardown".encode(), dest) #Teardown propagates through ring, reseting all information
            reply = s.recvfrom(1024)
            print reply[0]
            if command[0].decode() == "teardown-dht":   #If command is just a teardown, clear all information entirely
                for i in range(0, len(userStates)):
                    if (userStates[i] == "Leader") or (userStates[i] == "InDHT"):
                        userStates[i] == "Free"
                leader = None
            if command[0].decode() == "leave-dht":  #If command is leave-dht, reset the DHT with the same leader and one less user
                for i in range(0, len(userStates)): #Set all users except leader to free
                    if userStates[i] == "InDHT":
                        userStates[i] = "Free"
                    if users[i][0] == blockUser[0]: #Block user from being re-acquired by DHT
                        userStates[i] = "Blocked"
                s.sendto("reset-dht".encode(), dest)
                num = int(numUser[0]) - 1   
                s.sendto(str(num).encode(), dest)       #Sending command with one less
                s.sendto(sendLead[0].encode(), dest)    #Sending the leader

#Main function
#------------------------------------------------------------------------------------------------------
main()